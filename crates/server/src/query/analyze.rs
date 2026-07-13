//! ANALYZE (`docs/specs/statistics.md` §5–§6): reservoir sampling over a
//! snapshot-visible heap scan, the estimator math that turns a sample into
//! [`TableStatistics`], and the `run_analyze_pass` orchestration (target
//! resolution, AccessShare locking, and durable WAL/catalog publication).
//! The estimators are deterministic given a seed so tests can pin outputs.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::{
    ColumnId, ColumnStatistics, DbError, NDistinct, OrderedF64, QueryCancel, RelationKind, Result,
    Row, SqlState, StatementContext, TableSchema, TableStatistics, Value, value_is_finite,
};
use storage::{PageBackedStorageEngine, RelationSnapshot, StorageEngine};
use wal::{WalRecord, WalRecordKind};

use super::QueryService;
use super::gucs::DEFAULT_STATISTICS_TARGET_DEFAULT;
use super::txn::CapturedSnapshots;
use super::vacuum::{
    append_and_flush_maintenance_commit, cleanup_after_durable_maintenance_commit,
    fatal_after_durable_maintenance_commit, rollback_maintenance_txn_or_die,
};
use crate::app::ServerComponents;
use crate::lock_manager::{ObjectLockRequest, RelationLockMode};

/// Sample size per unit of `default_statistics_target`, matching PostgreSQL's
/// 300× rule (`docs/specs/statistics.md` §5).
pub const SAMPLE_ROWS_PER_TARGET: u32 = 300;

/// splitmix64 — a tiny deterministic PRNG, plenty for reservoir sampling and
/// dependency-free (`docs/specs/rust-style.md` discourages dependencies for
/// standard-library-sized helpers).
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform index in `[0, bound)` via the 128-bit multiply-shift reduction;
    /// `bound` must be non-zero.
    fn next_index(&mut self, bound: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(bound)) >> 64) as u64
    }
}

/// Sampled TEXT/BYTEA values wider than this are not retained: they still
/// count toward `null_frac` (as non-null) and `avg_width`, but are excluded
/// from the MCV list, histogram bounds, and the n_distinct estimator —
/// PostgreSQL's `WIDTH_THRESHOLD` analog. Without it, sampling a TOASTed
/// table of megabyte documents would hold `300 × target` fully materialized
/// rows in memory at once.
pub const WIDE_VALUE_THRESHOLD: u64 = 1024;

/// One sampled column value, with wide payloads reduced to their width.
#[derive(Clone, Debug, PartialEq)]
enum SampledValue {
    Null,
    /// A non-null value wider than [`WIDE_VALUE_THRESHOLD`]; only its width
    /// is retained.
    Wide {
        width: u64,
    },
    Value(Value),
}

/// One sampled row, columns in schema order.
type SampledRow = Vec<SampledValue>;

fn sampled_row(row: Row) -> SampledRow {
    row.values
        .into_iter()
        .map(|value| {
            if matches!(value, Value::Null) {
                return SampledValue::Null;
            }
            let width = value_width(&value);
            if width > WIDE_VALUE_THRESHOLD {
                SampledValue::Wide { width }
            } else {
                SampledValue::Value(value)
            }
        })
        .collect()
}

/// Algorithm R reservoir over scanned rows: after observing `k` rows, the
/// sample is a uniform random subset of them (capacity permitting), and the
/// exact count of observed rows is retained. Rows are stored width-capped
/// (see [`WIDE_VALUE_THRESHOLD`]), bounding reservoir memory.
pub struct RowReservoir {
    rng: SplitMix64,
    capacity: usize,
    rows_seen: u64,
    sample: Vec<SampledRow>,
}

impl RowReservoir {
    pub fn new(capacity: usize, seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
            capacity,
            rows_seen: 0,
            sample: Vec::new(),
        }
    }

    pub fn observe(&mut self, row: Row) {
        self.rows_seen += 1;
        if self.sample.len() < self.capacity {
            self.sample.push(sampled_row(row));
            return;
        }
        let slot = self.rng.next_index(self.rows_seen);
        if (slot as usize) < self.capacity {
            self.sample[slot as usize] = sampled_row(row);
        }
    }

    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    fn into_sample(self) -> Vec<SampledRow> {
        self.sample
    }
}

/// Scan `schema`'s heap under `ctx`'s snapshot and compute its statistics:
/// exact visible row count, heap page count, and per-column estimates from a
/// reservoir sample of `SAMPLE_ROWS_PER_TARGET × target` rows. Deterministic
/// given `seed`.
///
/// Memory stays bounded end to end: the engine's streaming
/// `for_each_visible_row` pass materializes (and detoasts) ONE row at a time —
/// unlike `scan`, which collects the whole table into a `Vec` before
/// returning its iterator — and the reservoir width-caps what it retains.
/// The engine's streaming pass checks cancellation per leaf page.
///
/// The output is finite by construction (`TableStatistics::is_finite`):
/// fractions come from non-zero denominators and non-finite sampled values are
/// excluded from MCVs and histogram bounds (§6).
pub fn collect_table_statistics(
    storage: &PageBackedStorageEngine,
    relations: &dyn RelationSnapshot,
    ctx: &StatementContext,
    schema: &TableSchema,
    target: u32,
    seed: u64,
) -> Result<TableStatistics> {
    let capacity = (SAMPLE_ROWS_PER_TARGET.saturating_mul(target)) as usize;
    let mut reservoir = RowReservoir::new(capacity, seed);
    // Explicitly the trait method with the caller-supplied relation snapshot
    // (captured under the caller's locks; the inherent helpers re-capture).
    StorageEngine::for_each_visible_row(storage, ctx, relations, schema.id, &mut |stored| {
        reservoir.observe(stored.row);
        Ok(())
    })?;

    let page_count = u64::from(storage.heap_page_count(schema)?);
    let row_count = reservoir.rows_seen();
    let sample = reservoir.into_sample();
    let columns = schema
        .columns
        .iter()
        .enumerate()
        .map(|(position, column)| {
            (
                column.id,
                column_statistics(&sample, position, row_count, target),
            )
        })
        .collect::<BTreeMap<ColumnId, ColumnStatistics>>();
    Ok(TableStatistics {
        row_count,
        page_count,
        columns,
    })
}

/// Estimators for one column over the sampled rows (`docs/specs/statistics.md`
/// §6). `total_rows` is the exact visible row count from the same scan.
///
/// Wide values (see [`WIDE_VALUE_THRESHOLD`]) count toward `null_frac` (as
/// non-null) and `avg_width`, but are excluded from the MCV list, histogram,
/// and the n_distinct estimator, which operate on the retained narrow values.
fn column_statistics(
    sample: &[SampledRow],
    position: usize,
    total_rows: u64,
    target: u32,
) -> ColumnStatistics {
    let mut narrow: Vec<&Value> = Vec::new();
    let mut non_null_count = 0u64;
    let mut total_width = 0u64;
    for row in sample {
        match row.get(position) {
            Some(SampledValue::Value(value)) => {
                narrow.push(value);
                non_null_count += 1;
                total_width += value_width(value);
            }
            Some(SampledValue::Wide { width }) => {
                non_null_count += 1;
                total_width += width;
            }
            Some(SampledValue::Null) | None => {}
        }
    }

    let null_frac = if sample.is_empty() {
        0.0
    } else {
        (sample.len() as u64 - non_null_count) as f64 / sample.len() as f64
    };

    let avg_width = total_width.checked_div(non_null_count).unwrap_or(0) as u32;

    // Sort once; every estimator below reads the (value, count) runs.
    narrow.sort();
    let mut runs: Vec<(&Value, usize)> = Vec::new();
    for value in &narrow {
        match runs.last_mut() {
            Some((run_value, count)) if *run_value == *value => *count += 1,
            _ => runs.push((value, 1)),
        }
    }

    let n_distinct = estimate_n_distinct(&runs, narrow.len(), total_rows);

    // MCVs: sampled values that occur more than once, most frequent first
    // (ties broken by value order for determinism). Frequencies are overall
    // fractions of the whole sample (nulls included), so downstream
    // selectivity math can combine them with `null_frac` directly. Non-finite
    // floats are excluded — the durable JSON encodings cannot round-trip them.
    let mut candidates: Vec<(&Value, usize)> = runs
        .iter()
        .filter(|(value, count)| *count >= 2 && value_is_finite(value))
        .copied()
        .collect();
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    candidates.truncate(target as usize);
    let most_common: Vec<(Value, OrderedF64)> = candidates
        .iter()
        .map(|(value, count)| {
            (
                (*value).clone(),
                OrderedF64::new(*count as f64 / sample.len() as f64),
            )
        })
        .collect();

    // Histogram: equi-height bounds over the finite sampled values not in the
    // MCV list, duplicates included.
    let mcv_values: BTreeSet<&Value> = candidates.iter().map(|(value, _)| *value).collect();
    let mut remaining: Vec<&Value> = Vec::new();
    for (value, count) in &runs {
        if value_is_finite(value) && !mcv_values.contains(*value) {
            remaining.extend(std::iter::repeat_n(*value, *count));
        }
    }
    let histogram_bounds = histogram_bounds(&remaining, target);

    ColumnStatistics {
        null_frac: OrderedF64::new(null_frac),
        avg_width,
        n_distinct,
        most_common,
        histogram_bounds,
    }
}

/// Distinct-value estimate (`docs/specs/statistics.md` §6): with sample size
/// `n`, distinct-in-sample `d`, singletons `f1`, and total live rows `N` —
/// no non-null values → `Count(0)`; all distinct → `Fraction(1.0)`; no
/// singletons (the sample likely saw every value) → `Count(d)`; otherwise the
/// Haas–Stokes estimator `D̂ = d·n / (n − f1 + f1·n/N)`, stored as a fraction
/// of `N` when it scales with the table (> 0.1·N).
fn estimate_n_distinct(runs: &[(&Value, usize)], n: usize, total_rows: u64) -> NDistinct {
    let d = runs.len();
    if n == 0 {
        return NDistinct::Count(0);
    }
    if d == n {
        return NDistinct::Fraction(OrderedF64::new(1.0));
    }
    let f1 = runs.iter().filter(|(_, count)| *count == 1).count();
    if f1 == 0 {
        return NDistinct::Count(d as u64);
    }
    let (n_f, d_f, f1_f) = (n as f64, d as f64, f1 as f64);
    // total_rows >= n > 0 (both counted by the same scan), so the denominator
    // is positive and the estimate finite.
    let total = total_rows.max(n as u64) as f64;
    let estimate = (d_f * n_f) / (n_f - f1_f + f1_f * n_f / total);
    let estimate = estimate.clamp(d_f, total);
    if estimate > 0.1 * total {
        NDistinct::Fraction(OrderedF64::new(estimate / total))
    } else {
        NDistinct::Count(estimate.round() as u64)
    }
}

/// Equi-height histogram bounds: `target + 1` evenly spaced positions over the
/// sorted remaining values (fewer when there are fewer values), first and last
/// included. Empty input → no histogram (the MCV list covered everything).
fn histogram_bounds(remaining: &[&Value], target: u32) -> Vec<Value> {
    if remaining.is_empty() {
        return Vec::new();
    }
    let bound_count = (target as usize + 1).min(remaining.len());
    if bound_count < 2 {
        // A histogram always has at least two bounds; a single leftover value
        // stores none (Milestone F's bucket interpolation divides by the
        // bucket count).
        return Vec::new();
    }
    let last = remaining.len() - 1;
    (0..bound_count)
        .map(|i| remaining[i * last / (bound_count - 1)].clone())
        .collect()
}

/// Approximate encoded width in bytes of one non-null value. Variable-length
/// kinds use their payload length; fixed-width kinds use their storage size.
/// Feeds `avg_width` (informational, `pg_stats.avg_width`), not correctness.
fn value_width(value: &Value) -> u64 {
    match value {
        Value::Null => 0,
        Value::Boolean(_) => 1,
        Value::Real(_) => 4,
        Value::Integer(_)
        | Value::Float(_)
        | Value::Date(_)
        | Value::Timestamp(_)
        | Value::Time(_)
        | Value::TimestampTz(_) => 8,
        Value::Numeric(_) | Value::Interval(_) | Value::Uuid(_) => 16,
        Value::Text(text) => text.len() as u64,
        Value::Bytes(bytes) => bytes.len() as u64,
        // Approximate: one byte of per-element overhead (so NULL-dense arrays
        // still have width proportional to their cardinality and cannot evade
        // the cap) plus the scalar element widths. Large arrays exceed
        // WIDE_VALUE_THRESHOLD and are width-capped like TEXT/BYTEA.
        Value::Array(array) => {
            array.elements().len() as u64 + array.elements().iter().map(value_width).sum::<u64>()
        }
    }
}

impl QueryService {
    /// Run the ANALYZE pass (`docs/specs/statistics.md` §5): collect
    /// statistics for one named user table or every user table, under
    /// `AccessShare` target locks (writers keep flowing; concurrent ANALYZE of
    /// the same table is a benign last-committed-wins race), and publish them
    /// durably — one `UpdateTableStatistics` WAL record per table plus the
    /// catalog update, in one maintenance transaction whose `Commit` is
    /// flushed before success is reported. A crash mid-pass applies none of
    /// the targets.
    pub(super) fn run_analyze_pass(
        &self,
        table: Option<String>,
        cancel: &Arc<QueryCancel>,
        statistics_target: u32,
    ) -> Result<()> {
        let components = &self.components;
        let mut discovered = {
            let _catalog_read = components
                .catalog_publication_gate
                .read()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            resolve_analyze_tables(components, table.as_deref())?
        };
        let txn_id = components
            .active_txns
            .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
        let writer_guard = match components.concurrency.begin_writer_cancelable(cancel) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut object_guard = match components.lock_manager.transaction_owner(txn_id) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        // Acquire-and-revalidate, like VACUUM: the target set discovered
        // before locking may have changed before the locks were granted.
        let baseline = object_guard.snapshot();
        let tables = loop {
            let requests = discovered
                .iter()
                .map(|schema| ObjectLockRequest::table(schema.id, RelationLockMode::AccessShare))
                .collect::<Vec<_>>();
            if let Err(err) = object_guard.acquire_many(&requests, cancel) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            let current = {
                let _catalog_read = match components.catalog_publication_gate.read() {
                    Ok(guard) => guard,
                    Err(_) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(DbError::internal("catalog publication gate poisoned"));
                    }
                };
                match resolve_analyze_tables(components, table.as_deref()) {
                    Ok(tables) => tables,
                    Err(err) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(err);
                    }
                }
            };
            if current
                .iter()
                .map(|schema| schema.id)
                .eq(discovered.iter().map(|schema| schema.id))
            {
                break current;
            }
            if let Err(err) = object_guard.restore(&baseline) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            discovered = current;
        };

        // A registered reader snapshot: its advertised xmin keeps a concurrent
        // VACUUM's GC horizon from reclaiming versions this scan still needs.
        let CapturedSnapshots {
            snapshot,
            relations,
            advertised,
        } = match self.capture_consistent_snapshots_cancelable(txn_id, cancel) {
            Ok(captured) => captured,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let ctx = StatementContext::with_snapshot(txn_id, snapshot)
            .with_conflict_waiter(components.lock_manager.clone(), cancel.clone());
        let seed = match analyze_seed() {
            Ok(seed) => seed,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut collected = Vec::with_capacity(tables.len());
        for schema in &tables {
            match collect_table_statistics(
                &components.storage,
                relations.as_ref(),
                &ctx,
                schema,
                statistics_target,
                seed,
            ) {
                Ok(statistics) => collected.push(statistics),
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            }
        }

        // Publish under the catalog publication gate: WAL record before the
        // in-memory catalog update (WAL-before-state, like DDL). The codec
        // and set_table_statistics both refuse non-finite payloads.
        {
            let _catalog_write = match components.catalog_publication_gate.write() {
                Ok(guard) => guard,
                Err(_) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(DbError::internal("catalog publication gate poisoned"));
                }
            };
            for (schema, statistics) in tables.iter().zip(&collected) {
                if let Err(err) = components.wal.append(WalRecord {
                    lsn: 0,
                    txn_id,
                    kind: WalRecordKind::UpdateTableStatistics {
                        table_id: schema.id,
                        statistics: statistics.clone(),
                    },
                }) {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
                if let Err(err) = components
                    .catalog
                    .set_table_statistics(schema.id, statistics.clone())
                {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            }
        }
        if let Err(err) = append_and_flush_maintenance_commit(components, txn_id) {
            self.rollback_pre_durable_or_die(txn_id, None);
            return Err(err);
        }
        if let Err(err) = cleanup_after_durable_maintenance_commit(components, txn_id) {
            fatal_after_durable_maintenance_commit(components, err);
        }
        components.active_txns.deregister(txn_id);
        components.lock_manager.on_txn_finished();
        drop(advertised);
        drop(object_guard);
        drop(writer_guard);
        // A full pass refreshed every table, so the auto-analyze accumulator
        // restarts (docs/specs/statistics.md §10); a single-table ANALYZE
        // leaves it alone.
        if table.is_none() {
            components
                .rows_changed_since_analyze
                .store(0, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }
}

/// Checkpoint auto-analyze (`docs/specs/statistics.md` §10): re-collect and
/// durably publish statistics for every user table, with the built-in default
/// statistics target and one committed maintenance transaction.
///
/// **Caller contract (like the checkpoint auto-prune):** the caller MUST hold
/// the EXCLUSIVE checkpoint guard, so no writer is in flight — the
/// sees-all-committed context is then exact — and MUST call this BEFORE the
/// checkpoint's `wal.flush()`/catalog snapshot, so the pass's WAL records are
/// flushed by this checkpoint and the manifest it writes carries the fresh
/// statistics (making the truncated records redundant).
pub(crate) fn checkpoint_auto_analyze(components: &ServerComponents) -> Result<()> {
    let tables = resolve_analyze_tables(components, None)?;
    if tables.is_empty() {
        return Ok(());
    }
    let txn_id = components
        .active_txns
        .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
    let ctx = StatementContext::new(txn_id);
    let result = (|| {
        let relations = components.storage.capture_relation_snapshot()?;
        let seed = analyze_seed()?;
        let mut collected = Vec::with_capacity(tables.len());
        for schema in &tables {
            collected.push(collect_table_statistics(
                &components.storage,
                relations.as_ref(),
                &ctx,
                schema,
                DEFAULT_STATISTICS_TARGET_DEFAULT,
                seed,
            )?);
        }
        let _catalog_write = components
            .catalog_publication_gate
            .write()
            .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
        for (schema, statistics) in tables.iter().zip(&collected) {
            components.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::UpdateTableStatistics {
                    table_id: schema.id,
                    statistics: statistics.clone(),
                },
            })?;
            components
                .catalog
                .set_table_statistics(schema.id, statistics.clone())?;
        }
        Ok(())
    })();
    if let Err(err) = result {
        rollback_maintenance_txn_or_die(components, txn_id);
        return Err(err);
    }
    if let Err(err) = append_and_flush_maintenance_commit(components, txn_id) {
        rollback_maintenance_txn_or_die(components, txn_id);
        return Err(err);
    }
    if let Err(err) = cleanup_after_durable_maintenance_commit(components, txn_id) {
        fatal_after_durable_maintenance_commit(components, err);
    }
    components.active_txns.deregister(txn_id);
    components.lock_manager.on_txn_finished();
    Ok(())
}

/// One named live user table, or every user table sorted by id. Hidden TOAST
/// relations are never analyzed: a named non-user relation reads as undefined.
fn resolve_analyze_tables(
    components: &ServerComponents,
    table: Option<&str>,
) -> Result<Vec<TableSchema>> {
    match table {
        Some(name) => components
            .catalog
            .get_table_by_name(name)?
            .filter(|schema| schema.relation_kind == RelationKind::User)
            .map(|schema| vec![schema])
            .ok_or_else(|| {
                DbError::plan(
                    SqlState::UndefinedTable,
                    format!("table {name} does not exist"),
                )
            }),
        None => {
            let mut tables = components
                .catalog
                .list_tables()?
                .into_iter()
                .filter(|schema| schema.relation_kind == RelationKind::User)
                .collect::<Vec<_>>();
            tables.sort_unstable_by_key(|schema| schema.id);
            Ok(tables)
        }
    }
}

/// Random sampler seed. The reservoir is deterministic given a seed; ANALYZE
/// draws a fresh one per pass so repeated runs sample independently.
fn analyze_seed() -> Result<u64> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes)
        .map_err(|err| DbError::internal(format!("failed to seed the ANALYZE sampler: {err}")))?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use common::{QueryCancel, StatementContext};

    use super::*;
    use crate::app::AppState;

    fn rows_of(values: Vec<Value>) -> Vec<Row> {
        values
            .into_iter()
            .map(|value| Row {
                values: vec![value],
            })
            .collect()
    }

    fn integer_rows(values: impl IntoIterator<Item = i64>) -> Vec<Row> {
        rows_of(values.into_iter().map(Value::Integer).collect())
    }

    /// Single-column sampled rows, width-capped exactly as `observe` stores
    /// them.
    fn sample_of(values: Vec<Value>) -> Vec<SampledRow> {
        rows_of(values).into_iter().map(sampled_row).collect()
    }

    #[test]
    fn reservoir_keeps_everything_under_capacity_and_counts_exactly() {
        let mut reservoir = RowReservoir::new(10, 7);
        for row in integer_rows(0..5) {
            reservoir.observe(row);
        }
        assert_eq!(reservoir.rows_seen(), 5);
        let sample = reservoir.into_sample();
        let expected: Vec<SampledRow> = integer_rows(0..5).into_iter().map(sampled_row).collect();
        assert_eq!(sample, expected);
    }

    #[test]
    fn reservoir_is_capacity_bounded_deterministic_and_samples_the_input() {
        let run = |seed: u64| {
            let mut reservoir = RowReservoir::new(8, seed);
            for row in integer_rows(0..1000) {
                reservoir.observe(row);
            }
            assert_eq!(reservoir.rows_seen(), 1000);
            reservoir.into_sample()
        };
        let first = run(42);
        assert_eq!(first.len(), 8);
        assert_eq!(first, run(42), "same seed must reproduce the same sample");
        for row in &first {
            let SampledValue::Value(Value::Integer(v)) = row[0] else {
                panic!("unexpected value kind");
            };
            assert!((0..1000).contains(&v), "sample must come from the input");
        }
    }

    #[test]
    fn unique_integer_column_is_fraction_one_with_histogram_only() {
        let sample = sample_of((0..200).map(Value::Integer).collect());
        let stats = column_statistics(&sample, 0, 200, 10);
        assert_eq!(stats.null_frac, OrderedF64::new(0.0));
        assert_eq!(stats.avg_width, 8);
        assert_eq!(stats.n_distinct, NDistinct::Fraction(OrderedF64::new(1.0)));
        assert!(stats.most_common.is_empty(), "unique column has no MCVs");
        assert_eq!(stats.histogram_bounds.len(), 11, "target + 1 bounds");
        assert_eq!(stats.histogram_bounds[0], Value::Integer(0));
        assert_eq!(stats.histogram_bounds[10], Value::Integer(199));
    }

    #[test]
    fn skewed_text_column_yields_mcvs_and_histogram_over_the_rest() {
        // 50× "heavy", 20× "medium", singletons "s00".."s29".
        let mut values = vec![Value::Text("heavy".to_string()); 50];
        values.extend(vec![Value::Text("medium".to_string()); 20]);
        values.extend((0..30).map(|i| Value::Text(format!("s{i:02}"))));
        let sample = sample_of(values);
        let stats = column_statistics(&sample, 0, 100, 10);

        assert_eq!(stats.most_common.len(), 2);
        assert_eq!(stats.most_common[0].0, Value::Text("heavy".to_string()));
        assert_eq!(stats.most_common[0].1, OrderedF64::new(0.5));
        assert_eq!(stats.most_common[1].0, Value::Text("medium".to_string()));
        assert_eq!(stats.most_common[1].1, OrderedF64::new(0.2));
        assert!(
            !stats.histogram_bounds.is_empty()
                && !stats
                    .histogram_bounds
                    .contains(&Value::Text("heavy".to_string())),
            "histogram covers the non-MCV remainder only"
        );
    }

    #[test]
    fn all_null_column_has_no_value_statistics() {
        let sample = sample_of(vec![Value::Null; 40]);
        let stats = column_statistics(&sample, 0, 40, 10);
        assert_eq!(stats.null_frac, OrderedF64::new(1.0));
        assert_eq!(stats.avg_width, 0);
        assert_eq!(stats.n_distinct, NDistinct::Count(0));
        assert!(stats.most_common.is_empty());
        assert!(stats.histogram_bounds.is_empty());
    }

    #[test]
    fn mostly_null_column_reports_the_null_fraction() {
        let mut values = vec![Value::Null; 75];
        values.extend((0..25).map(Value::Integer));
        let stats = column_statistics(&sample_of(values), 0, 100, 10);
        assert_eq!(stats.null_frac, OrderedF64::new(0.75));
        assert_eq!(stats.n_distinct, NDistinct::Fraction(OrderedF64::new(1.0)));
    }

    #[test]
    fn repeated_only_column_is_an_exact_count_with_no_histogram() {
        // Five values, twenty occurrences each: no singletons, so the sample
        // likely saw every distinct value.
        let values: Vec<Value> = (0..5).flat_map(|v| vec![Value::Integer(v); 20]).collect();
        let stats = column_statistics(&sample_of(values), 0, 100, 10);
        assert_eq!(stats.n_distinct, NDistinct::Count(5));
        assert_eq!(stats.most_common.len(), 5, "MCV list covers all values");
        assert!(
            stats.histogram_bounds.is_empty(),
            "nothing remains outside the MCV list"
        );
    }

    #[test]
    fn haas_stokes_estimate_stays_between_sample_distinct_and_total() {
        // Mixed: some repeats, some singletons, table larger than the sample.
        let mut values: Vec<Value> = (0..20).flat_map(|v| vec![Value::Integer(v); 3]).collect();
        values.extend((100..140).map(Value::Integer));
        let n = values.len() as u64; // 100 sampled
        let stats = column_statistics(&sample_of(values), 0, 10_000, 10);
        match stats.n_distinct {
            NDistinct::Count(count) => {
                assert!((60..=10_000).contains(&count), "count {count} out of range");
            }
            NDistinct::Fraction(fraction) => {
                let implied = fraction.get() * 10_000.0;
                assert!(
                    (60.0..=10_000.0).contains(&implied),
                    "implied distinct {implied} out of range (n = {n})"
                );
            }
        }
    }

    #[test]
    fn non_finite_doubles_are_excluded_from_mcvs_and_histogram() {
        let mut values = vec![Value::Float(OrderedF64::new(f64::NAN)); 30];
        values.extend(vec![Value::Float(OrderedF64::new(f64::INFINITY)); 30]);
        values.extend((0..40).map(|v| Value::Float(OrderedF64::new(f64::from(v)))));
        let sample = sample_of(values);
        let stats = column_statistics(&sample, 0, 100, 10);

        assert!(
            stats.most_common.is_empty(),
            "the only repeated values are non-finite and must be excluded"
        );
        assert!(
            stats.histogram_bounds.iter().all(value_is_finite),
            "histogram bounds must be finite"
        );
        // Non-finite values still count toward n_distinct (2 of 42 distinct).
        assert_ne!(stats.n_distinct, NDistinct::Count(40));
        let table = TableStatistics {
            row_count: 100,
            page_count: 1,
            columns: BTreeMap::from([(0, stats)]),
        };
        assert!(
            table.is_finite(),
            "collector output must always pass the durable-boundary guard"
        );
    }

    #[test]
    fn text_widths_average_over_non_null_values() {
        let values = vec![
            Value::Text("ab".to_string()),
            Value::Text("abcd".to_string()),
            Value::Null,
        ];
        let stats = column_statistics(&sample_of(values), 0, 3, 10);
        assert_eq!(stats.avg_width, 3);
    }

    #[test]
    fn null_dense_arrays_are_width_capped() {
        // A NULL element still costs one byte of width, so '{NULL,NULL,...}'
        // with thousands of elements is reduced to SampledValue::Wide instead
        // of being retained fully materialized (and can never reach MCVs or
        // histogram bounds).
        use common::{ArrayDimension, SqlArray};
        let wide_array = Value::Array(
            SqlArray::new(
                common::DataType::Double,
                vec![ArrayDimension::new(2000, 1)],
                vec![Value::Null; 2000],
            )
            .unwrap(),
        );
        assert!(value_width(&wide_array) > WIDE_VALUE_THRESHOLD);
        let sample = sample_of(vec![wide_array.clone(), wide_array, Value::Null]);
        let stats = column_statistics(&sample, 0, 3, 10);
        assert!(
            stats.most_common.is_empty(),
            "a wide array is not retained, so it cannot become an MCV"
        );
        assert!(stats.avg_width as u64 >= 2000);
    }

    #[test]
    fn mcv_frequency_is_a_fraction_of_the_whole_sample() {
        // 50 NULLs + 50× 'x': the MCV frequency is the overall fraction (0.5),
        // not the non-null fraction (1.0), so Σ mcv_freqs + null_frac ≤ 1 and
        // the downstream equality-miss selectivity stays non-negative.
        let mut values = vec![Value::Null; 50];
        values.extend(vec![Value::Text("x".to_string()); 50]);
        let stats = column_statistics(&sample_of(values), 0, 100, 10);
        assert_eq!(stats.null_frac, OrderedF64::new(0.5));
        assert_eq!(
            stats.most_common,
            vec![(Value::Text("x".to_string()), OrderedF64::new(0.5))]
        );
    }

    #[test]
    fn wide_values_count_for_width_and_nulls_but_not_value_statistics() {
        let wide = "w".repeat(2048);
        let mut values = vec![Value::Text(wide.clone()); 10];
        values.extend((0..20).map(|i| Value::Text(format!("v{i:02}"))));
        values.extend(vec![Value::Null; 5]);
        let stats = column_statistics(&sample_of(values), 0, 35, 10);

        assert_eq!(stats.null_frac, OrderedF64::new(5.0 / 35.0));
        // (10 × 2048 + 20 × 3) bytes over 30 non-null values.
        assert_eq!(stats.avg_width, ((10 * 2048 + 20 * 3) / 30) as u32);
        assert!(
            stats.most_common.is_empty(),
            "the only repeated value is wide and must not be retained"
        );
        assert!(
            stats.histogram_bounds.iter().all(|bound| match bound {
                Value::Text(text) => text.len() <= WIDE_VALUE_THRESHOLD as usize,
                _ => false,
            }),
            "histogram bounds come from narrow values only"
        );
        // n_distinct sees only the narrow values — a documented caveat shared
        // with PostgreSQL's WIDTH_THRESHOLD.
        assert_eq!(stats.n_distinct, NDistinct::Fraction(OrderedF64::new(1.0)));
    }

    #[test]
    fn collects_statistics_over_a_real_table_under_mvcc_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        let mut insert = String::from("insert into users (id, name) values ");
        for id in 0..100 {
            if id > 0 {
                insert.push(',');
            }
            // Four heavily repeated names.
            insert.push_str(&format!("({id}, 'name{}')", id % 4));
        }
        app.query_service.execute_sql(&insert).unwrap();

        let schema = app
            .components
            .catalog
            .get_table_by_name("users")
            .unwrap()
            .unwrap();
        let relations = app.components.storage.capture_relation_snapshot().unwrap();
        let ctx = StatementContext::new(0);
        let stats = collect_table_statistics(
            &app.components.storage,
            relations.as_ref(),
            &ctx,
            &schema,
            100,
            7,
        )
        .unwrap();

        assert_eq!(stats.row_count, 100);
        assert!(stats.page_count >= 1);
        let id_stats = &stats.columns[&0];
        assert_eq!(
            id_stats.n_distinct,
            NDistinct::Fraction(OrderedF64::new(1.0))
        );
        let name_stats = &stats.columns[&1];
        assert_eq!(name_stats.n_distinct, NDistinct::Count(4));
        assert_eq!(name_stats.most_common.len(), 4);
        assert!(stats.columns.values().all(|c| c.null_frac.get() == 0.0));

        // Deleted rows are invisible to a later snapshot: the counts follow.
        app.query_service
            .execute_sql("delete from users where id >= 50")
            .unwrap();
        let relations = app.components.storage.capture_relation_snapshot().unwrap();
        let stats = collect_table_statistics(
            &app.components.storage,
            relations.as_ref(),
            &ctx,
            &schema,
            100,
            7,
        )
        .unwrap();
        assert_eq!(stats.row_count, 50);
    }

    #[test]
    fn collection_scan_honors_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table big (id integer primary key)")
            .unwrap();
        let mut insert = String::from("insert into big (id) values ");
        for id in 0..2048 {
            if id > 0 {
                insert.push(',');
            }
            insert.push_str(&format!("({id})"));
        }
        app.query_service.execute_sql(&insert).unwrap();

        let schema = app
            .components
            .catalog
            .get_table_by_name("big")
            .unwrap()
            .unwrap();
        let relations = app.components.storage.capture_relation_snapshot().unwrap();
        let mut ctx = StatementContext::new(0);
        let cancel = std::sync::Arc::new(QueryCancel::new());
        cancel.request(common::CancelReason::UserRequest);
        ctx.cancel = cancel;
        let err = collect_table_statistics(
            &app.components.storage,
            relations.as_ref(),
            &ctx,
            &schema,
            100,
            7,
        )
        .unwrap_err();
        assert_eq!(err.code, common::SqlState::QueryCanceled);
    }
}
