use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use common::{DbError, IsolationLevel, Result, SqlState};
use executor::{CopyIn, CopyJob, CopyOut, ExecutionContext};
use tokio::sync::mpsc;

use super::{
    CapturedSnapshots, CopySnapshots, ExecutionContextInput, QueryService, QuerySessionContext,
    Transaction, TransactionSnapshots, WriteUnitGuard,
};
use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;

/// One inbound COPY-from-stdin event, sent by the connection loop to the blocking
/// COPY transaction task. `Done` (clean end-of-input) commits; `Fail` (client
/// `CopyFail`) aborts the statement; a dropped channel means the client
/// disconnected and the task must abort any owned transaction itself.
pub enum CopyInChunk {
    Chunk(Vec<u8>),
    Done,
    Fail,
}

enum CopyInError {
    Db(DbError),
    Disconnected,
}

impl From<DbError> for CopyInError {
    fn from(err: DbError) -> Self {
        Self::Db(err)
    }
}

/// Target size for an outbound `COPY ... TO STDOUT` `CopyData` frame; rows are
/// batched up to this before being sent, well under the protocol frame cap.
const COPY_OUT_FRAME_BYTES: usize = 64 * 1024;

impl QueryService {
    /// Run a `COPY ... FROM STDIN` to completion, owning the transaction. The
    /// connection loop feeds `rx` from `CopyData`/`CopyDone`/`CopyFail`. Returns
    /// the (possibly still-open, in-transaction) slot and the rows inserted.
    pub(crate) fn run_copy_in_stream(
        &self,
        job: CopyJob,
        slot: Option<Transaction>,
        session: QuerySessionContext,
        snapshots: CopySnapshots,
        rx: mpsc::Receiver<CopyInChunk>,
    ) -> (Option<Transaction>, Result<u64>) {
        match (slot, snapshots) {
            (
                None,
                CopySnapshots::Autocommit {
                    snapshots,
                    write_guard,
                },
            ) => (
                None,
                self.copy_in_autocommit(job, session, snapshots, write_guard, rx),
            ),
            (Some(txn), CopySnapshots::Transaction(snapshots)) => {
                self.copy_in_transaction(txn, job, session, snapshots, rx)
            }
            (slot, _) => (
                slot,
                Err(DbError::internal(
                    "COPY FROM snapshot mode did not match transaction state",
                )),
            ),
        }
    }

    /// Autocommit COPY FROM: its own transaction, all-or-nothing (mirrors
    /// `autocommit_write`, but the execute step is the streaming insert loop and
    /// COPY FROM produces no committed dead versions).
    fn copy_in_autocommit(
        &self,
        job: CopyJob,
        session: QuerySessionContext,
        snapshots: CapturedSnapshots,
        write_guard: Option<WriteUnitGuard>,
        rx: mpsc::Receiver<CopyInChunk>,
    ) -> Result<u64> {
        let CapturedSnapshots {
            snapshot,
            relations,
            advertised: _advertised,
        } = snapshots;
        let guard = match write_guard {
            Some(guard) => guard,
            None => WriteUnitGuard::Shared(self.components.concurrency.begin_writer()?),
        };
        let txn_id = self.register_active_txn();
        let gc_horizon = self.components.gc_horizon();
        // Autocommit COPY FROM: a fresh txn with no savepoints, so the live-set is
        // just `[txn_id]`.
        let ctx = self.execution_context(ExecutionContextInput {
            txn_id,
            snapshot,
            relations,
            isolation: IsolationLevel::default(),
            gc_horizon,
            live_txns: Arc::from([txn_id]),
            runtime: session
                .statement_runtime(IsolationLevel::default(), IsolationLevel::default()),
        })?;

        let outcome = catch_unwind(AssertUnwindSafe(|| drive_copy_in(&ctx, job, rx)));
        let count = match outcome {
            Ok(Ok(count)) => count,
            Ok(Err(CopyInError::Db(err))) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            Ok(Err(CopyInError::Disconnected)) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(copy_disconnected_error());
            }
            Err(_) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(DbError::internal("COPY FROM execution panicked"));
            }
        };

        if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
            self.rollback_pre_durable_or_die(txn_id, None);
            return Err(err);
        }
        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        self.components.active_txns.deregister(txn_id);
        // Wake any writer blocked on this committed COPY's row locks.
        self.components.lock_manager.on_txn_finished();
        drop(guard);
        // COPY FROM only inserts, so it produces no committed dead versions; still
        // count the commit toward the checkpoint trigger.
        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);
        Ok(count)
    }

    /// COPY FROM inside an explicit transaction: fold into the open transaction
    /// (lazy write guard, the transaction's snapshot, no commit). Mirrors
    /// `run_bound_in_transaction`'s write handling.
    fn copy_in_transaction(
        &self,
        mut txn: Transaction,
        job: CopyJob,
        session: QuerySessionContext,
        snapshots: TransactionSnapshots,
        rx: mpsc::Receiver<CopyInChunk>,
    ) -> (Option<Transaction>, Result<u64>) {
        if txn.write_guard.is_none()
            && let Err(err) = self.acquire_write_guard(&mut txn)
        {
            txn.failed = true;
            return (Some(txn), Err(err));
        }
        let TransactionSnapshots {
            snapshot,
            relations,
            advertised,
        } = snapshots;
        txn.first_statement_ran = true;
        let gc_horizon = self.components.gc_horizon();
        let result = (|| {
            // COPY FROM may run inside a transaction with open savepoints: stamp
            // inserts with the innermost subxid and thread the live (sub)xid set.
            let ctx = self.execution_context(ExecutionContextInput {
                txn_id: txn.writing_xid(),
                snapshot,
                relations,
                isolation: txn.isolation,
                gc_horizon,
                live_txns: txn.live_txns(),
                runtime: session.statement_runtime(
                    txn.current_default_isolation(IsolationLevel::default()),
                    txn.isolation,
                ),
            })?;
            let result = drive_copy_in(&ctx, job, rx);
            drop(ctx);
            result
        })();
        drop(advertised);
        match result {
            // COPY FROM inserts produce no committed dead versions (dead_versions += 0).
            Ok(count) => (Some(txn), Ok(count)),
            Err(CopyInError::Db(err)) => {
                txn.failed = true;
                (Some(txn), Err(err))
            }
            Err(CopyInError::Disconnected) => {
                self.abort_transaction(txn);
                (None, Err(copy_disconnected_error()))
            }
        }
    }

    /// Run a `COPY ... TO STDOUT` to completion, pushing rendered frames into
    /// `frame_tx`. Returns the (possibly still-open) slot and the rows exported.
    pub(crate) fn run_copy_out_stream(
        &self,
        job: CopyJob,
        slot: Option<Transaction>,
        session: QuerySessionContext,
        snapshots: CopySnapshots,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> (Option<Transaction>, Result<u64>) {
        match (slot, snapshots) {
            (
                None,
                CopySnapshots::Autocommit {
                    snapshots,
                    write_guard: _write_guard,
                },
            ) => (
                None,
                self.copy_out_autocommit(job, session, snapshots, frame_tx),
            ),
            (Some(txn), CopySnapshots::Transaction(snapshots)) => {
                self.copy_out_transaction(txn, job, session, snapshots, frame_tx)
            }
            (slot, _) => (
                slot,
                Err(DbError::internal(
                    "COPY TO snapshot mode did not match transaction state",
                )),
            ),
        }
    }

    /// Autocommit COPY TO: a lock-free read with its own snapshot (mirrors
    /// `autocommit_read`); the advertisement is held for the whole scan.
    fn copy_out_autocommit(
        &self,
        job: CopyJob,
        session: QuerySessionContext,
        snapshots: CapturedSnapshots,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<u64> {
        let CapturedSnapshots {
            snapshot,
            relations,
            advertised: _advertised,
        } = snapshots;
        let ctx = self.execution_context(ExecutionContextInput {
            txn_id: 0,
            snapshot,
            relations,
            isolation: IsolationLevel::default(),
            gc_horizon: 0,
            live_txns: Arc::from([0]),
            runtime: session
                .statement_runtime(IsolationLevel::default(), IsolationLevel::default()),
        })?;
        drive_copy_out(&ctx, job, frame_tx)
    }

    /// COPY TO inside an explicit transaction: read with the transaction's
    /// snapshot. A read error poisons the block, matching other statements.
    fn copy_out_transaction(
        &self,
        mut txn: Transaction,
        job: CopyJob,
        session: QuerySessionContext,
        snapshots: TransactionSnapshots,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> (Option<Transaction>, Result<u64>) {
        let TransactionSnapshots {
            snapshot,
            relations,
            advertised,
        } = snapshots;
        txn.first_statement_ran = true;
        let result = (|| {
            // A read inside a savepoint transaction must see its own subxids' writes,
            // so thread the live (sub)xid set.
            let ctx = self.execution_context(ExecutionContextInput {
                txn_id: txn.writing_xid(),
                snapshot,
                relations,
                isolation: txn.isolation,
                gc_horizon: 0,
                live_txns: txn.live_txns(),
                runtime: session.statement_runtime(
                    txn.current_default_isolation(IsolationLevel::default()),
                    txn.isolation,
                ),
            })?;
            let result = drive_copy_out(&ctx, job, frame_tx);
            drop(ctx);
            result
        })();
        drop(advertised);
        match result {
            Ok(count) => (Some(txn), Ok(count)),
            Err(err) => {
                txn.failed = true;
                (Some(txn), Err(err))
            }
        }
    }
}

/// Pull COPY-from-stdin chunks until a clean `Done` (returns the rows inserted),
/// a client abort (`Fail`), or a dropped channel (disconnect).
fn drive_copy_in(
    ctx: &ExecutionContext<'_>,
    job: CopyJob,
    mut rx: mpsc::Receiver<CopyInChunk>,
) -> std::result::Result<u64, CopyInError> {
    let mut copy_in = CopyIn::new(
        ctx,
        job.schema,
        job.columns,
        job.options,
        job.default_exprs,
        job.check_exprs,
    )?;
    loop {
        match rx.blocking_recv() {
            Some(CopyInChunk::Chunk(bytes)) => copy_in.push_chunk(&bytes)?,
            Some(CopyInChunk::Done) => return copy_in.finish().map_err(CopyInError::from),
            // The connection loop substitutes the client's message for a CopyFail.
            Some(CopyInChunk::Fail) => {
                return Err(CopyInError::Db(DbError::execute(
                    SqlState::QueryCanceled,
                    "COPY from stdin aborted",
                )));
            }
            // A disconnect has no session left to receive a returned transaction
            // slot, so the caller must abort the transaction itself.
            None => return Err(CopyInError::Disconnected),
        }
    }
}

fn copy_disconnected_error() -> DbError {
    DbError::execute(
        SqlState::QueryCanceled,
        "COPY from stdin client disconnected",
    )
}

/// Scan + project + render COPY-to-stdout rows, batching into frames pushed to
/// `frame_tx`. Returns the rows exported. A dropped receiver (client gone) ends
/// the scan with an error.
fn drive_copy_out(
    ctx: &ExecutionContext<'_>,
    job: CopyJob,
    frame_tx: mpsc::Sender<Vec<u8>>,
) -> Result<u64> {
    let mut out = CopyOut::new(ctx, job.schema, &job.columns, job.options)?;
    let mut frame = Vec::new();
    if let Some(header) = out.header_line() {
        frame.extend_from_slice(&header);
    }
    let mut count = 0u64;
    while let Some(row) = out.next_row()? {
        frame.extend_from_slice(&row);
        count += 1;
        if frame.len() >= COPY_OUT_FRAME_BYTES {
            let full = std::mem::take(&mut frame);
            if frame_tx.blocking_send(full).is_err() {
                return Err(DbError::io("COPY to stdout client disconnected"));
            }
        }
    }
    if !frame.is_empty() && frame_tx.blocking_send(frame).is_err() {
        return Err(DbError::io("COPY to stdout client disconnected"));
    }
    Ok(count)
}
