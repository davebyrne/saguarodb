//! The SELECT streaming bridge: a bounded channel carrying result rows from the
//! blocking producer (which owns the `PlanExecutor`) to the async connection task
//! that writes them to the socket (`docs/specs/streaming.md`).

use std::ops::ControlFlow;

use common::{ColumnInfo, Result, Row};
use executor::{ExecutionResult, RowSink};
use tokio::sync::mpsc;

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

/// The outcome of a (possibly streaming) statement execution.
#[derive(Debug)]
pub enum StreamOutcome {
    /// A SELECT whose rows were streamed through the channel. `count` is the
    /// authoritative number of rows produced by the drive (used for the
    /// `SELECT n` command tag).
    Streamed { count: u64 },
    /// Any non-streamed result — DML, DDL, EXPLAIN, a COPY request, or a SELECT
    /// run on the materializing path — returned in full.
    Direct(ExecutionResult),
    /// A non-streamed result that also requires connection-scoped session objects
    /// outside the query service (prepared statements and portals) to be reset.
    SessionReset(ExecutionResult),
}

impl StreamOutcome {
    /// Unwrap a non-streamed result. A `Streamed` outcome only arises when a row
    /// sink was supplied, which the materializing (sink-less) callers never do,
    /// so this cannot panic on those paths.
    pub(crate) fn expect_direct(self) -> ExecutionResult {
        match self {
            StreamOutcome::Direct(result) | StreamOutcome::SessionReset(result) => result,
            StreamOutcome::Streamed { .. } => {
                unreachable!("a streamed outcome cannot arise without a row sink")
            }
        }
    }
}

/// A [`RowSink`] that forwards streamed SELECT output over a bounded channel to
/// the async connection task. `blocking_send` applies backpressure (it blocks the
/// producer thread while the channel is full); a dropped receiver (client gone)
/// turns the next push into a graceful stop rather than an error, so a mere
/// disconnect never poisons an open transaction. Mirrors the COPY-out driver.
pub(crate) struct ChannelRowSink {
    tx: mpsc::Sender<StreamMessage>,
}

impl ChannelRowSink {
    pub(crate) fn new(tx: mpsc::Sender<StreamMessage>) -> Self {
        Self { tx }
    }
}

impl RowSink for ChannelRowSink {
    fn start(&mut self, columns: &[ColumnInfo]) -> Result<()> {
        // Best effort: if the receiver is already gone the connection is dead, and
        // the first `push` will stop the scan. There is no useful error to raise.
        let _ = self.tx.blocking_send(StreamMessage::Start {
            columns: columns.to_vec(),
        });
        Ok(())
    }

    fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>> {
        match self.tx.blocking_send(StreamMessage::Rows(rows)) {
            Ok(()) => Ok(ControlFlow::Continue(())),
            // Receiver dropped: the consumer is gone. Stop gracefully.
            Err(_) => Ok(ControlFlow::Break(())),
        }
    }
}
