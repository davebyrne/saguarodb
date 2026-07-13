//! The SELECT streaming bridge: a bounded channel carrying result rows from the
//! blocking producer (which owns the `PlanExecutor`) to the async connection task
//! that writes them to the socket (`docs/specs/streaming.md`).

use std::fmt;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::{ColumnInfo, DbError, QueryCancel, Result, Row, SqlState};
use executor::{CopyJob, ExecutionResult, RowSink};
use tokio::sync::mpsc;

use super::{CapturedSnapshots, TransactionSnapshots, WriteUnitGuard};
use crate::app::ServerComponents;
use crate::lock_manager::ObjectLockGuard;

pub(crate) struct AutocommitCopyWrite {
    components: Arc<ServerComponents>,
    txn_id: Option<u64>,
    object_guard: Option<ObjectLockGuard>,
    write_guard: Option<WriteUnitGuard>,
}

impl AutocommitCopyWrite {
    pub(crate) fn new(
        components: Arc<ServerComponents>,
        txn_id: u64,
        object_guard: ObjectLockGuard,
        write_guard: WriteUnitGuard,
    ) -> Self {
        Self {
            components,
            txn_id: Some(txn_id),
            object_guard: Some(object_guard),
            write_guard: Some(write_guard),
        }
    }

    pub(crate) fn txn_id(&self) -> Result<u64> {
        self.txn_id
            .ok_or_else(|| DbError::internal("COPY write ownership is no longer armed"))
    }

    pub(crate) fn disarm(&mut self) {
        self.txn_id = None;
    }
}

impl Drop for AutocommitCopyWrite {
    fn drop(&mut self) {
        if let Some(txn_id) = self.txn_id.take() {
            super::QueryService::new(self.components.clone())
                .rollback_pre_durable_or_die(txn_id, None);
        }
        // Fields drop after this body: object locks first, then the shared writer
        // guard, preserving reverse acquisition order.
        self.object_guard.take();
        self.write_guard.take();
    }
}

/// Rows pulled per `PlanExecutor` batch and carried per channel message
/// (`docs/specs/streaming.md` §7). A tuning knob only: it bounds peak buffered
/// rows (about `STREAM_CHANNEL_CAPACITY` × this) and affects neither correctness
/// nor the wire protocol.
pub(crate) const STREAM_BATCH_ROWS: usize = 64;

/// Bounded capacity of the row-stream channel (`docs/specs/overview.md`
/// §Query Result Architecture). Bounding the channel is what provides
/// backpressure and the memory ceiling.
pub const STREAM_CHANNEL_CAPACITY: usize = 64;

/// A message on the SELECT row-stream channel, from the blocking producer to the
/// async connection task (`docs/specs/streaming.md` §4).
pub enum StreamMessage {
    /// The output schema, sent once before any rows — even for an empty result,
    /// so the consumer always emits a `RowDescription`.
    Start { columns: Vec<ColumnInfo> },
    /// A batch of result rows.
    Rows(Vec<Row>),
}

/// The snapshots captured after COPY binding, object-lock acquisition, and
/// revalidation. COPY crosses the query and protocol layers, so the snapshots and
/// lock owner must cross with it through stream completion.
pub(crate) enum CopySnapshots {
    Autocommit {
        snapshots: CapturedSnapshots,
        catalog: Arc<dyn catalog::CatalogManager>,
        write: Option<AutocommitCopyWrite>,
        object_guard: Option<ObjectLockGuard>,
    },
    Transaction {
        snapshots: TransactionSnapshots,
        catalog: Arc<dyn catalog::CatalogManager>,
        catalog_is_snapshot: bool,
    },
}

impl fmt::Debug for CopySnapshots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopySnapshots::Autocommit { .. } => f.write_str("CopySnapshots::Autocommit(..)"),
            CopySnapshots::Transaction { .. } => f.write_str("CopySnapshots::Transaction(..)"),
        }
    }
}

/// The outcome of a (possibly streaming) statement execution.
#[derive(Debug)]
pub(crate) enum StreamOutcome {
    /// A SELECT whose rows were streamed through the channel. `count` is the
    /// authoritative number of rows produced by the drive (used for the
    /// `SELECT n` command tag).
    Streamed { count: u64 },
    /// Any non-streamed result — DML, DDL, EXPLAIN, or a SELECT run on the
    /// materializing path — returned in full.
    Direct(ExecutionResult),
    /// A non-streamed result that crossed its durable or irreversible session-state
    /// completion boundary. A timeout noticed slightly later by the async consumer
    /// must not replace this success with an error.
    Durable(ExecutionResult),
    /// `COPY ... FROM STDIN`: the connection loop sends `CopyInResponse` and
    /// streams client data while holding `snapshots` for the COPY lifetime.
    BeginCopyIn {
        job: CopyJob,
        snapshots: CopySnapshots,
    },
    /// `COPY ... TO STDOUT`: the connection loop sends `CopyOutResponse` and
    /// streams table data while holding `snapshots` for the COPY lifetime.
    BeginCopyOut {
        job: CopyJob,
        snapshots: CopySnapshots,
    },
    /// A non-streamed result that also requires connection-scoped session objects
    /// outside the query service (prepared statements and portals) to be reset.
    SessionReset(ExecutionResult),
}

impl StreamOutcome {
    /// Convert a non-streamed result for callers that cannot drive protocol-level
    /// streaming. `SELECT` cannot stream without a row sink, but COPY still needs
    /// the connection sub-protocol, so surface a structured error instead of
    /// panicking.
    pub(crate) fn into_direct_result(self) -> Result<ExecutionResult> {
        match self {
            StreamOutcome::Direct(result)
            | StreamOutcome::Durable(result)
            | StreamOutcome::SessionReset(result) => Ok(result),
            StreamOutcome::BeginCopyIn { .. } | StreamOutcome::BeginCopyOut { .. } => {
                Err(DbError::plan(
                    SqlState::FeatureNotSupported,
                    "COPY requires the PostgreSQL COPY sub-protocol",
                ))
            }
            StreamOutcome::Streamed { .. } => Err(DbError::internal(
                "streamed query result requires protocol-level driving",
            )),
        }
    }
}

/// A [`RowSink`] that forwards streamed SELECT output over a bounded channel to
/// the async connection task. Retrying `try_send` applies backpressure while
/// polling cancellation; a dropped receiver (client gone) turns the next push
/// into a graceful stop rather than an error, so a mere disconnect never poisons
/// an open transaction. Mirrors the COPY-out driver.
pub(crate) struct ChannelRowSink {
    tx: mpsc::Sender<StreamMessage>,
    cancel: Arc<QueryCancel>,
}

impl ChannelRowSink {
    pub(crate) fn new(tx: mpsc::Sender<StreamMessage>, cancel: Arc<QueryCancel>) -> Self {
        Self { tx, cancel }
    }

    fn send(&self, message: StreamMessage) -> Result<bool> {
        send_cancelable(&self.tx, self.cancel.as_ref(), message)
    }
}

pub(crate) fn send_cancelable<T>(
    sender: &mpsc::Sender<T>,
    cancel: &QueryCancel,
    mut message: T,
) -> Result<bool> {
    loop {
        cancel.check()?;
        match sender.try_send(message) {
            Ok(()) => return Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                cancel.check()?;
                return Ok(false);
            }
            Err(mpsc::error::TrySendError::Full(returned)) => {
                message = returned;
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

impl RowSink for ChannelRowSink {
    fn start(&mut self, columns: &[ColumnInfo]) -> Result<()> {
        let _ = self.send(StreamMessage::Start {
            columns: columns.to_vec(),
        })?;
        Ok(())
    }

    fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>> {
        if self.send(StreamMessage::Rows(rows))? {
            Ok(ControlFlow::Continue(()))
        } else {
            Ok(ControlFlow::Break(()))
        }
    }
}
