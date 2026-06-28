use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::{DbError, IsolationLevel, Result, SqlState};
use executor::{CopyIn, CopyJob, CopyOut, ExecutionContext};
use tokio::sync::mpsc;

use super::{QueryService, Transaction, WriteUnitGuard};
use crate::checkpoint::record_commit_and_maybe_checkpoint;

/// One inbound COPY-from-stdin event, sent by the connection loop to the blocking
/// COPY transaction task. `Done` (clean end-of-input) commits; `Fail` (client
/// `CopyFail`) and a dropped channel (client disconnect) both abort.
pub enum CopyInChunk {
    Chunk(Vec<u8>),
    Done,
    Fail,
}

/// Target size for an outbound `COPY ... TO STDOUT` `CopyData` frame; rows are
/// batched up to this before being sent, well under the protocol frame cap.
const COPY_OUT_FRAME_BYTES: usize = 64 * 1024;

impl QueryService {
    /// Run a `COPY ... FROM STDIN` to completion, owning the transaction. The
    /// connection loop feeds `rx` from `CopyData`/`CopyDone`/`CopyFail`. Returns
    /// the (possibly still-open, in-transaction) slot and the rows inserted.
    pub fn run_copy_in_stream(
        &self,
        job: CopyJob,
        slot: Option<Transaction>,
        cancel: &Arc<AtomicBool>,
        rx: mpsc::Receiver<CopyInChunk>,
    ) -> (Option<Transaction>, Result<u64>) {
        match slot {
            None => (None, self.copy_in_autocommit(job, cancel, rx)),
            Some(txn) => self.copy_in_transaction(txn, job, cancel, rx),
        }
    }

    /// Autocommit COPY FROM: its own transaction, all-or-nothing (mirrors
    /// `autocommit_write`, but the execute step is the streaming insert loop and
    /// COPY FROM produces no committed dead versions).
    fn copy_in_autocommit(
        &self,
        job: CopyJob,
        cancel: &Arc<AtomicBool>,
        rx: mpsc::Receiver<CopyInChunk>,
    ) -> Result<u64> {
        let guard = WriteUnitGuard::Shared(self.components.concurrency.begin_writer()?);
        let txn_id = self.register_active_txn();
        let (snapshot, _advertised) = self.capture_snapshot(txn_id);
        let gc_horizon = self.components.gc_horizon();
        // Autocommit COPY FROM: a fresh txn with no savepoints, so the live-set is
        // just `[txn_id]`.
        let ctx = self.execution_context(
            txn_id,
            snapshot,
            IsolationLevel::default(),
            gc_horizon,
            Arc::from([txn_id]),
            cancel,
        );

        let outcome = catch_unwind(AssertUnwindSafe(|| drive_copy_in(&ctx, job, rx)));
        let count = match outcome {
            Ok(Ok(count)) => count,
            Ok(Err(err)) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
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
        drop(guard);
        // COPY FROM only inserts, so it produces no committed dead versions; still
        // count the commit toward the checkpoint trigger.
        if let Err(err) = record_commit_and_maybe_checkpoint(&self.components) {
            eprintln!("checkpoint failed after committed COPY: {err}");
        }
        Ok(count)
    }

    /// COPY FROM inside an explicit transaction: fold into the open transaction
    /// (lazy write guard, the transaction's snapshot, no commit). Mirrors
    /// `run_bound_in_transaction`'s write handling.
    fn copy_in_transaction(
        &self,
        mut txn: Transaction,
        job: CopyJob,
        cancel: &Arc<AtomicBool>,
        rx: mpsc::Receiver<CopyInChunk>,
    ) -> (Option<Transaction>, Result<u64>) {
        if txn.write_guard.is_none()
            && let Err(err) = self.acquire_write_guard(&mut txn)
        {
            txn.failed = true;
            return (Some(txn), Err(err));
        }
        let (snapshot, advertised) = self.snapshot_for_transaction(&mut txn);
        txn.first_statement_ran = true;
        let gc_horizon = self.components.gc_horizon();
        let result = {
            // COPY FROM may run inside a transaction with open savepoints: stamp
            // inserts with the innermost subxid and thread the live (sub)xid set.
            let ctx = self.execution_context(
                txn.writing_xid(),
                snapshot,
                txn.isolation,
                gc_horizon,
                txn.live_txns(),
                cancel,
            );
            let result = drive_copy_in(&ctx, job, rx);
            drop(ctx);
            result
        };
        drop(advertised);
        match result {
            // COPY FROM inserts produce no committed dead versions (dead_versions += 0).
            Ok(count) => (Some(txn), Ok(count)),
            Err(err) => {
                txn.failed = true;
                (Some(txn), Err(err))
            }
        }
    }

    /// Run a `COPY ... TO STDOUT` to completion, pushing rendered frames into
    /// `frame_tx`. Returns the (possibly still-open) slot and the rows exported.
    pub fn run_copy_out_stream(
        &self,
        job: CopyJob,
        slot: Option<Transaction>,
        cancel: &Arc<AtomicBool>,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> (Option<Transaction>, Result<u64>) {
        match slot {
            None => (None, self.copy_out_autocommit(job, cancel, frame_tx)),
            Some(txn) => self.copy_out_transaction(txn, job, cancel, frame_tx),
        }
    }

    /// Autocommit COPY TO: a lock-free read with its own snapshot (mirrors
    /// `autocommit_read`); the advertisement is held for the whole scan.
    fn copy_out_autocommit(
        &self,
        job: CopyJob,
        cancel: &Arc<AtomicBool>,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<u64> {
        let (snapshot, _advertised) = self.capture_snapshot(0);
        let ctx = self.execution_context(
            0,
            snapshot,
            IsolationLevel::default(),
            0,
            Arc::from([0]),
            cancel,
        );
        drive_copy_out(&ctx, job, frame_tx)
    }

    /// COPY TO inside an explicit transaction: read with the transaction's
    /// snapshot. A read error poisons the block, matching other statements.
    fn copy_out_transaction(
        &self,
        mut txn: Transaction,
        job: CopyJob,
        cancel: &Arc<AtomicBool>,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> (Option<Transaction>, Result<u64>) {
        let (snapshot, advertised) = self.snapshot_for_transaction(&mut txn);
        txn.first_statement_ran = true;
        let result = {
            // A read inside a savepoint transaction must see its own subxids' writes,
            // so thread the live (sub)xid set.
            let ctx = self.execution_context(
                txn.writing_xid(),
                snapshot,
                txn.isolation,
                0,
                txn.live_txns(),
                cancel,
            );
            let result = drive_copy_out(&ctx, job, frame_tx);
            drop(ctx);
            result
        };
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

/// Pull COPY-from-stdin chunks until a clean `Done` (returns the rows inserted)
/// or an abort (`Fail`/dropped channel → error, which the caller rolls back).
fn drive_copy_in(
    ctx: &ExecutionContext<'_>,
    job: CopyJob,
    mut rx: mpsc::Receiver<CopyInChunk>,
) -> Result<u64> {
    let mut copy_in = CopyIn::new(ctx, job.table, job.columns, job.options)?;
    loop {
        match rx.blocking_recv() {
            Some(CopyInChunk::Chunk(bytes)) => copy_in.push_chunk(&bytes)?,
            Some(CopyInChunk::Done) => return copy_in.finish(),
            // `Fail` (client CopyFail) or a dropped sender (disconnect): abort. The
            // connection loop substitutes the client's message for a CopyFail.
            Some(CopyInChunk::Fail) | None => {
                return Err(DbError::execute(
                    SqlState::QueryCanceled,
                    "COPY from stdin aborted",
                ));
            }
        }
    }
}

/// Scan + project + render COPY-to-stdout rows, batching into frames pushed to
/// `frame_tx`. Returns the rows exported. A dropped receiver (client gone) ends
/// the scan with an error.
fn drive_copy_out(
    ctx: &ExecutionContext<'_>,
    job: CopyJob,
    frame_tx: mpsc::Sender<Vec<u8>>,
) -> Result<u64> {
    let mut out = CopyOut::new(ctx, job.table, &job.columns, job.options)?;
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
