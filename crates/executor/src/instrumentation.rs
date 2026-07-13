use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ColumnInfo, ExecRow, Result};
use planner::{NodeExecutionMetrics, PlanNodeId, PlanNodeLayout};

use crate::query::PlanExecutor;
use crate::subquery::AnalysisState;

#[derive(Clone, Default)]
pub(crate) struct MetricCollector {
    nodes: Arc<Mutex<BTreeMap<PlanNodeId, NodeExecutionMetrics>>>,
}

#[derive(Clone)]
pub(crate) struct DynamicProfile {
    pub(crate) layout: PlanNodeLayout,
    pub(crate) collector: MetricCollector,
    pub(crate) analysis: Option<AnalysisState>,
    pub(crate) init_parent: Option<usize>,
}

impl MetricCollector {
    pub(crate) fn snapshot(&self) -> BTreeMap<PlanNodeId, NodeExecutionMetrics> {
        self.nodes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn merge(&self, id: PlanNodeId, local: LoopMetrics) {
        let mut nodes = self
            .nodes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let node = nodes.entry(id).or_default();
        node.loops = node.loops.saturating_add(1);
        node.rows = node.rows.saturating_add(local.rows);
        node.startup = node.startup.saturating_add(local.startup);
        node.total = node.total.saturating_add(local.total);
    }
}

#[derive(Default)]
struct LoopMetrics {
    rows: u64,
    startup: Duration,
    total: Duration,
    fetched: bool,
}

pub(crate) struct InstrumentedExecutor<'a> {
    inner: Box<dyn PlanExecutor + 'a>,
    id: PlanNodeId,
    collector: MetricCollector,
    current: Option<LoopMetrics>,
}

impl<'a> InstrumentedExecutor<'a> {
    pub(crate) fn new(
        inner: Box<dyn PlanExecutor + 'a>,
        id: PlanNodeId,
        collector: MetricCollector,
    ) -> Self {
        Self {
            inner,
            id,
            collector,
            current: None,
        }
    }

    fn record_fetch(&mut self, elapsed: Duration, rows: usize) {
        let Some(current) = &mut self.current else {
            return;
        };
        current.total = current.total.saturating_add(elapsed);
        if !current.fetched {
            current.startup = current.startup.saturating_add(elapsed);
            current.fetched = true;
        }
        current.rows = current
            .rows
            .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
    }

    fn merge_current(&mut self) {
        let Some(mut current) = self.current.take() else {
            return;
        };
        if !current.fetched {
            current.startup = current.total;
        }
        self.collector.merge(self.id, current);
    }
}

impl PlanExecutor for InstrumentedExecutor<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        self.inner.output_schema()
    }

    fn open(&mut self) -> Result<()> {
        self.merge_current();
        let started = Instant::now();
        let result = self.inner.open();
        let elapsed = started.elapsed();
        if result.is_ok() {
            self.current = Some(LoopMetrics {
                startup: elapsed,
                total: elapsed,
                ..LoopMetrics::default()
            });
        }
        result
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let started = Instant::now();
        let result = self.inner.next();
        let elapsed = started.elapsed();
        let rows = usize::from(matches!(result, Ok(Some(_))));
        self.record_fetch(elapsed, rows);
        result
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ExecRow>> {
        let started = Instant::now();
        let result = self.inner.next_batch(max_rows);
        let elapsed = started.elapsed();
        let rows = result.as_ref().map_or(0, Vec::len);
        self.record_fetch(elapsed, rows);
        result
    }

    fn close(&mut self) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.close();
        if let Some(current) = &mut self.current {
            current.total = current.total.saturating_add(started.elapsed());
        }
        self.merge_current();
        result
    }
}

impl Drop for InstrumentedExecutor<'_> {
    fn drop(&mut self) {
        self.merge_current();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::thread;

    use catalog::MemoryCatalog;
    use common::{DataType, DbError, QueryCancel, Row, StatementContext, Value};
    use planner::{ApplyKind, PhysicalPlan};
    use storage::StorageEngine;

    use super::*;
    use crate::query::{ExecutionContext, build_executor, build_executor_with_profile};
    use crate::test_support::MemoryStorage;

    struct FakeLeaf {
        schema: Vec<ColumnInfo>,
        row_count: usize,
        remaining: usize,
        fail_open: bool,
        batch_calls: Option<Rc<Cell<usize>>>,
        delay: Duration,
    }

    impl FakeLeaf {
        fn rows(row_count: usize) -> Self {
            Self {
                schema: vec![ColumnInfo {
                    name: "n".to_string(),
                    data_type: DataType::Integer,
                    table_id: None,
                    column_id: None,
                    pg_type: None,
                }],
                row_count,
                remaining: 0,
                fail_open: false,
                batch_calls: None,
                delay: Duration::ZERO,
            }
        }

        fn take_row(&mut self) -> Option<ExecRow> {
            if self.remaining == 0 {
                return None;
            }
            self.remaining -= 1;
            Some(ExecRow {
                row: Row {
                    values: vec![Value::Integer(self.remaining as i64)],
                },
                identity: None,
            })
        }
    }

    impl PlanExecutor for FakeLeaf {
        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            if self.fail_open {
                return Err(DbError::internal("open failed"));
            }
            self.remaining = self.row_count;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<ExecRow>> {
            if !self.delay.is_zero() {
                thread::sleep(self.delay);
            }
            Ok(self.take_row())
        }

        fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ExecRow>> {
            if let Some(calls) = &self.batch_calls {
                calls.set(calls.get() + 1);
            }
            Ok((0..max_rows).filter_map(|_| self.take_row()).collect())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn metric(collector: &MetricCollector, id: usize) -> NodeExecutionMetrics {
        collector.snapshot()[&PlanNodeId(id)].clone()
    }

    #[test]
    fn counts_rows_and_aggregates_successful_open_loops() {
        let collector = MetricCollector::default();
        let mut executor = InstrumentedExecutor::new(
            Box::new(FakeLeaf::rows(3)),
            PlanNodeId(0),
            collector.clone(),
        );
        for _ in 0..2 {
            executor.open().unwrap();
            while executor.next().unwrap().is_some() {}
            executor.close().unwrap();
        }

        let metric = metric(&collector, 0);
        assert_eq!(metric.loops, 2);
        assert_eq!(metric.rows, 6);
        assert!(metric.total >= metric.startup);
    }

    #[test]
    fn failed_open_does_not_count_a_loop() {
        let collector = MetricCollector::default();
        let mut leaf = FakeLeaf::rows(1);
        leaf.fail_open = true;
        let mut executor =
            InstrumentedExecutor::new(Box::new(leaf), PlanNodeId(0), collector.clone());

        assert!(executor.open().is_err());
        executor.close().unwrap();
        assert!(collector.snapshot().is_empty());
    }

    #[test]
    fn early_and_duplicate_close_merge_once() {
        let collector = MetricCollector::default();
        let mut executor = InstrumentedExecutor::new(
            Box::new(FakeLeaf::rows(3)),
            PlanNodeId(0),
            collector.clone(),
        );
        executor.open().unwrap();
        assert!(executor.next().unwrap().is_some());
        executor.close().unwrap();
        executor.close().unwrap();
        drop(executor);

        let metric = metric(&collector, 0);
        assert_eq!(metric.loops, 1);
        assert_eq!(metric.rows, 1);
    }

    #[test]
    fn drop_merges_an_open_loop_without_more_operator_work() {
        let collector = MetricCollector::default();
        let mut executor = InstrumentedExecutor::new(
            Box::new(FakeLeaf::rows(2)),
            PlanNodeId(0),
            collector.clone(),
        );
        executor.open().unwrap();
        assert!(executor.next().unwrap().is_some());
        drop(executor);

        assert_eq!(metric(&collector, 0).rows, 1);
    }

    #[test]
    fn next_batch_counts_rows_and_calls_the_inner_batch_method() {
        let collector = MetricCollector::default();
        let calls = Rc::new(Cell::new(0));
        let mut leaf = FakeLeaf::rows(3);
        leaf.batch_calls = Some(calls.clone());
        let mut executor =
            InstrumentedExecutor::new(Box::new(leaf), PlanNodeId(0), collector.clone());
        executor.open().unwrap();
        assert_eq!(executor.next_batch(2).unwrap().len(), 2);
        executor.close().unwrap();

        assert_eq!(calls.get(), 1);
        assert_eq!(metric(&collector, 0).rows, 2);
    }

    struct Parent<'a> {
        child: Box<dyn PlanExecutor + 'a>,
    }

    impl PlanExecutor for Parent<'_> {
        fn output_schema(&self) -> &[ColumnInfo] {
            self.child.output_schema()
        }

        fn open(&mut self) -> Result<()> {
            self.child.open()
        }

        fn next(&mut self) -> Result<Option<ExecRow>> {
            self.child.next()
        }

        fn close(&mut self) -> Result<()> {
            self.child.close()
        }
    }

    #[test]
    fn parent_timing_is_inclusive_of_child_calls() {
        let collector = MetricCollector::default();
        let mut leaf = FakeLeaf::rows(1);
        leaf.delay = Duration::from_millis(1);
        let child = InstrumentedExecutor::new(Box::new(leaf), PlanNodeId(1), collector.clone());
        let mut parent = InstrumentedExecutor::new(
            Box::new(Parent {
                child: Box::new(child),
            }),
            PlanNodeId(0),
            collector.clone(),
        );
        parent.open().unwrap();
        while parent.next().unwrap().is_some() {}
        parent.close().unwrap();

        assert!(metric(&collector, 0).total >= metric(&collector, 1).total);
    }

    #[test]
    fn uninstrumented_builder_does_not_touch_a_collector() {
        let catalog = Arc::new(MemoryCatalog::empty());
        let storage = MemoryStorage::empty();
        let cancel = QueryCancel::new();
        let ctx = ExecutionContext {
            statement: StatementContext::new(0),
            relations: storage.capture_relation_snapshot().unwrap(),
            catalog,
            storage: &storage,
            schema_ops: &storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        let plan = PhysicalPlan::Values {
            rows: vec![vec![]],
            output_schema: Vec::new(),
        };
        let collector = MetricCollector::default();
        let mut executor = build_executor(&ctx, &plan).unwrap();
        executor.open().unwrap();
        assert!(executor.next().unwrap().is_some());
        executor.close().unwrap();

        assert!(collector.snapshot().is_empty());
    }

    #[test]
    fn profiled_builder_rejects_a_layout_with_the_wrong_shape() {
        let catalog = Arc::new(MemoryCatalog::empty());
        let storage = MemoryStorage::empty();
        let cancel = QueryCancel::new();
        let ctx = ExecutionContext {
            statement: StatementContext::new(0),
            relations: storage.capture_relation_snapshot().unwrap(),
            catalog,
            storage: &storage,
            schema_ops: &storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        let leaf = PhysicalPlan::Values {
            rows: vec![vec![]],
            output_schema: Vec::new(),
        };
        let plan = PhysicalPlan::Projection {
            source: Box::new(leaf.clone()),
            expressions: Vec::new(),
            output_schema: Vec::new(),
        };
        let wrong_layout = PlanNodeLayout::new(&leaf);

        let err = match build_executor_with_profile(
            &ctx,
            &plan,
            &wrong_layout,
            &MetricCollector::default(),
        ) {
            Ok(_) => panic!("mismatched layout should fail"),
            Err(err) => err,
        };
        assert!(err.message.contains("plan/layout mismatch"));
    }

    #[test]
    fn profiled_builder_validates_an_apply_subplan_before_outer_execution() {
        let catalog = Arc::new(MemoryCatalog::empty());
        let storage = MemoryStorage::empty();
        let cancel = QueryCancel::new();
        let ctx = ExecutionContext {
            statement: StatementContext::new(0),
            relations: storage.capture_relation_snapshot().unwrap(),
            catalog,
            storage: &storage,
            schema_ops: &storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        let empty_input = PhysicalPlan::Values {
            rows: Vec::new(),
            output_schema: Vec::new(),
        };
        let leaf_subplan = PhysicalPlan::Values {
            rows: vec![vec![]],
            output_schema: Vec::new(),
        };
        let layout_plan = PhysicalPlan::Apply {
            input: Box::new(empty_input.clone()),
            subplan: Box::new(leaf_subplan.clone()),
            correlations: Vec::new(),
            kind: ApplyKind::Exists { negated: false },
        };
        let actual_plan = PhysicalPlan::Apply {
            input: Box::new(empty_input),
            subplan: Box::new(PhysicalPlan::Projection {
                source: Box::new(leaf_subplan),
                expressions: Vec::new(),
                output_schema: Vec::new(),
            }),
            correlations: Vec::new(),
            kind: ApplyKind::Exists { negated: false },
        };

        let err = match build_executor_with_profile(
            &ctx,
            &actual_plan,
            &PlanNodeLayout::new(&layout_plan),
            &MetricCollector::default(),
        ) {
            Ok(_) => panic!("Apply subplan mismatch should fail before open"),
            Err(err) => err,
        };
        assert!(err.message.contains("plan/layout mismatch"));
    }
}
