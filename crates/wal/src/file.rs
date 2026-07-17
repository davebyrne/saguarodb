use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use common::{
    DbError, FIRST_NORMAL_XID, Lsn, Result, SqlState, TxnId, TxnStatus, TxnStatusView, WalPosition,
};

use crate::clog_file::{ClogSnapshot, MAX_CLOG_FILE_BYTES, decode_clog, encode_clog};
use crate::codec::{HEADER_LEN, encode_record_at, frame_len_from_header};
#[cfg(test)]
use crate::segment::MetaWriteFailure;
use crate::segment::{
    SEGMENT_PAYLOAD_BYTES, SegmentReader, WalMeta, active_segment_number, append_stream,
    discover_end, initialize, load_meta, open_segment, recycle_orphaned_segments, recycle_segments,
    segment_path, sync_dir, truncate_stream, wal_dir, write_meta_with_outcome,
};
use crate::{Clog, WalEntry, WalManager, WalRecord, WalRecordKind, decode_record};

pub struct FileWalManager {
    data_dir: PathBuf,
    wal_dir: PathBuf,
    state: Mutex<WalState>,
}

struct WalState {
    active: File,
    active_number: u64,
    unflushed_first_segment: Option<u64>,
    replay_floor: Lsn,
    written_lsn: Lsn,
    flushed_lsn: Lsn,
    clog: Clog,
    pending_commits: HashSet<u64>,
    vacuum_floor: TxnId,
    clog_loaded_from_snapshot: bool,
    recycle_boundary: Option<Lsn>,
    poisoned: Option<DbError>,
    #[cfg(test)]
    fail_next_flush: Option<String>,
    #[cfg(test)]
    fail_next_post_write_seek: Option<String>,
    #[cfg(test)]
    fail_next_parent_sync: Option<String>,
    #[cfg(test)]
    fail_next_durable_end_sync: Option<String>,
}

impl FileWalManager {
    /// Open the segmented WAL rooted in `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|err| {
            DbError::io(format!(
                "failed to create data directory {}: {err}",
                data_dir.display()
            ))
        })?;
        if data_dir.join("wal.dat").exists() {
            return Err(wal_error(
                "unsupported legacy single-file WAL; rebuild the data directory",
            ));
        }
        initialize(&data_dir)?;
        let wal_dir = wal_dir(&data_dir);
        let meta = load_meta(&wal_dir)?;
        recycle_orphaned_segments(&wal_dir, meta.replay_floor)?;
        if meta.replay_floor > meta.durable_end {
            return Err(wal_error("WAL replay floor is beyond its durable end"));
        }
        let physical_end = discover_end(&wal_dir, meta.replay_floor, meta.durable_end)?;
        if meta.durable_end > physical_end {
            return Err(wal_error("WAL durable end is beyond the retained stream"));
        }
        if physical_end > meta.durable_end {
            drop(truncate_stream(
                &wal_dir,
                meta.durable_end,
                physical_end,
                meta.replay_floor,
            )?);
        }
        let end = meta.durable_end;

        let clog_path = data_dir.join("clog.dat");
        let snapshot = load_clog_snapshot(&clog_path)?;
        let recycle_boundary = snapshot
            .as_ref()
            .map(|snapshot| snapshot.authorized_replay_floor);
        let (mut clog, vacuum_floor, clog_loaded_from_snapshot, clog_lsn) = match snapshot {
            Some(snapshot)
                if snapshot.clog_lsn < meta.replay_floor
                    || snapshot.authorized_replay_floor < meta.replay_floor =>
            {
                return Err(wal_error(
                    "CLOG snapshot does not cover the retained WAL replay floor",
                ));
            }
            Some(snapshot) if snapshot.clog_lsn > end => {
                return Err(wal_error("CLOG snapshot is ahead of the WAL stream"));
            }
            Some(snapshot) => (
                Clog::from_snapshot(&snapshot),
                snapshot.vacuum_floor,
                true,
                snapshot.clog_lsn,
            ),
            None if meta.replay_floor == 0 => (Clog::new(), FIRST_NORMAL_XID, false, 0),
            None => {
                return Err(wal_error(
                    "CLOG snapshot is missing after WAL segments were recycled",
                ));
            }
        };

        scan_stream(&wal_dir, clog_lsn, end, |record| {
            fold_status(&mut clog, record);
            Ok(())
        })?;
        let next_number = end / SEGMENT_PAYLOAD_BYTES;
        let active_number = if end.is_multiple_of(SEGMENT_PAYLOAD_BYTES)
            && segment_path(&wal_dir, next_number).exists()
        {
            next_number
        } else {
            active_segment_number(end)?
        };
        let active = open_segment(&wal_dir, active_number)?;

        Ok(Self {
            data_dir,
            wal_dir,
            state: Mutex::new(WalState {
                active,
                active_number,
                unflushed_first_segment: None,
                replay_floor: meta.replay_floor,
                written_lsn: end,
                flushed_lsn: end,
                clog,
                pending_commits: HashSet::new(),
                vacuum_floor,
                clog_loaded_from_snapshot,
                recycle_boundary,
                poisoned: None,
                #[cfg(test)]
                fail_next_flush: None,
                #[cfg(test)]
                fail_next_post_write_seek: None,
                #[cfg(test)]
                fail_next_parent_sync: None,
                #[cfg(test)]
                fail_next_durable_end_sync: None,
            }),
        })
    }

    fn lock_raw_state(&self) -> Result<std::sync::MutexGuard<'_, WalState>> {
        self.state
            .lock()
            .map_err(|_| DbError::internal("WAL manager lock was poisoned"))
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, WalState>> {
        let state = self.lock_raw_state()?;
        if let Some(error) = &state.poisoned {
            return Err(error.clone());
        }
        Ok(state)
    }

    fn clog_path(&self) -> PathBuf {
        self.data_dir.join("clog.dat")
    }

    fn repair_failed_append(
        &self,
        state: &mut WalState,
        start: Lsn,
        attempted_end: Lsn,
    ) -> Result<()> {
        match truncate_stream(&self.wal_dir, start, attempted_end, state.replay_floor) {
            Ok((active, active_number)) => {
                state.active = active;
                state.active_number = active_number;
                if state
                    .unflushed_first_segment
                    .is_some_and(|first| first > state.active_number)
                {
                    state.unflushed_first_segment = None;
                }
                Ok(())
            }
            Err(err) => {
                state.poisoned = Some(err.clone());
                Err(err)
            }
        }
    }

    fn correct_failed_flush(&self, state: &mut WalState) -> Result<()> {
        let durable_end = state.flushed_lsn;
        let (active, active_number) = truncate_stream(
            &self.wal_dir,
            durable_end,
            state.written_lsn,
            state.replay_floor,
        )?;
        state.active = active;
        state.active_number = active_number;
        state.written_lsn = durable_end;
        state.pending_commits.clear();
        state.unflushed_first_segment = None;
        Ok(())
    }

    fn sync_unflushed(&self, state: &WalState) -> Result<()> {
        let Some(mut number) = state.unflushed_first_segment else {
            return Ok(());
        };
        loop {
            if number == state.active_number {
                state.active.sync_all().map_err(|err| {
                    DbError::io(format!("failed to fsync active WAL segment: {err}"))
                })?;
            } else {
                open_segment(&self.wal_dir, number)?
                    .sync_all()
                    .map_err(|err| {
                        DbError::io(format!("failed to fsync filled WAL segment: {err}"))
                    })?;
            }
            if number == state.active_number {
                return Ok(());
            }
            number = number
                .checked_add(1)
                .ok_or_else(|| wal_error("WAL segment number overflow"))?;
        }
    }
}

impl WalManager for FileWalManager {
    fn append_positioned(&self, record: WalRecord) -> Result<WalPosition> {
        let mut state = self.lock_raw_state()?;
        let is_settlement = matches!(
            record.kind,
            WalRecordKind::Commit | WalRecordKind::CommitWithSubxids { .. } | WalRecordKind::Abort
        );
        if represents_transaction(&record)
            && !is_settlement
            && state.clog.must_backpressure_new_writer(record.txn_id)
        {
            return Err(DbError::wal(
                SqlState::ProgramLimitExceeded,
                "transaction status window is pinned by an active writer; retry after older transactions finish",
            ));
        }
        if matches!(record.kind, WalRecordKind::Abort) {
            state.clog.set_aborted(record.txn_id);
        } else if represents_transaction(&record) {
            // Make every writer explicit before its first record can be absorbed
            // by a concurrent checkpoint CLOG snapshot. This includes transactions
            // allocated after the checkpoint's active-id/allocation capture.
            state.clog.set_in_progress(record.txn_id);
        }
        if let Some(error) = &state.poisoned {
            return Err(error.clone());
        }
        let start = state.written_lsn;
        let (record, bytes) = encode_record_at(record, start)?;
        let attempted_end = record.lsn;
        let append_result = {
            let WalState {
                active,
                active_number,
                ..
            } = &mut *state;
            append_stream(&self.wal_dir, active, active_number, start, &bytes)
        };
        match append_result {
            Ok(()) => {}
            Err(err) => {
                self.repair_failed_append(&mut state, start, attempted_end)?;
                return Err(err);
            }
        }
        state
            .unflushed_first_segment
            .get_or_insert(start / SEGMENT_PAYLOAD_BYTES);

        #[cfg(test)]
        if let Some(message) = state.fail_next_post_write_seek.take() {
            self.repair_failed_append(&mut state, start, attempted_end)?;
            return Err(DbError::io(message));
        }

        match &record.kind {
            WalRecordKind::Commit => {
                state.pending_commits.insert(record.txn_id);
            }
            WalRecordKind::CommitWithSubxids { subxids } => {
                state.pending_commits.insert(record.txn_id);
                state.pending_commits.extend(subxids.iter().copied());
            }
            _ => {}
        }
        state.written_lsn = record.lsn;
        WalPosition::new(start, record.lsn)
    }

    fn flush(&self) -> Result<Lsn> {
        let mut state = self.lock_state()?;
        let sync_result = {
            #[cfg(test)]
            {
                if let Some(message) = state.fail_next_flush.take() {
                    Err(DbError::io(message))
                } else {
                    self.sync_unflushed(&state)
                }
            }
            #[cfg(not(test))]
            {
                self.sync_unflushed(&state)
            }
        };
        if let Err(err) = sync_result {
            if let Err(correction) = self.correct_failed_flush(&mut state) {
                state.poisoned = Some(correction.clone());
                return Err(correction);
            }
            state.poisoned = Some(wal_error(format!(
                "WAL manager is unavailable after durability failure: {}",
                err.message
            )));
            return Err(err);
        }
        if state.written_lsn > state.flushed_lsn {
            let meta_result = write_meta_with_outcome(
                &self.wal_dir,
                WalMeta {
                    replay_floor: state.replay_floor,
                    durable_end: state.written_lsn,
                },
            );
            #[cfg(test)]
            let meta_result = {
                let mut result = meta_result;
                if result.is_ok()
                    && let Some(message) = state.fail_next_durable_end_sync.take()
                {
                    result = Err(MetaWriteFailure {
                        error: DbError::io(message),
                        replacement_visible: true,
                    });
                }
                result
            };
            if let Err(failure) = meta_result {
                if failure.replacement_visible {
                    let error = DbError::durability_outcome_unknown(format!(
                        "WAL durable-end replacement has an unknown persistence outcome: {}",
                        failure.error.message
                    ));
                    state.poisoned = Some(error.clone());
                    return Err(error);
                }
                if let Err(correction) = self.correct_failed_flush(&mut state) {
                    state.poisoned = Some(correction.clone());
                    return Err(correction);
                }
                state.poisoned = Some(wal_error(format!(
                    "WAL manager is unavailable after durable-end update failure: {}",
                    failure.error.message
                )));
                return Err(failure.error);
            }
        }
        state.flushed_lsn = state.written_lsn;
        let pending = std::mem::take(&mut state.pending_commits);
        for txn_id in pending {
            state.clog.set_committed(txn_id);
        }
        state.unflushed_first_segment = None;
        Ok(state.flushed_lsn)
    }

    fn written_lsn(&self) -> Result<Lsn> {
        Ok(self.lock_state()?.written_lsn)
    }

    fn replay_entries_from(
        &self,
        replay_from: Lsn,
    ) -> Result<Box<dyn Iterator<Item = Result<WalEntry>>>> {
        let state = self.lock_state()?;
        Ok(Box::new(WalReplay {
            reader: SegmentReader::new(self.wal_dir.clone(), state.replay_floor, state.written_lsn),
            threshold: replay_from.max(state.replay_floor),
            done: false,
        }))
    }

    fn recycle_through(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.lock_state()?;
        if lsn < state.replay_floor || lsn > state.flushed_lsn {
            return Err(wal_error(
                "WAL recycle boundary is outside the durable retained range",
            ));
        }
        if lsn == state.replay_floor {
            return recycle_orphaned_segments(&self.wal_dir, lsn);
        }
        if state.recycle_boundary != Some(lsn) {
            return Err(wal_error(
                "WAL recycle boundary was not established by the latest CLOG snapshot",
            ));
        }
        if lsn == state.written_lsn && lsn.is_multiple_of(SEGMENT_PAYLOAD_BYTES) {
            let number = lsn / SEGMENT_PAYLOAD_BYTES;
            if !segment_path(&self.wal_dir, number).exists() {
                state.active = crate::segment::create_segment(&self.wal_dir, number)?;
                state.active_number = number;
            }
        }
        if let Err(failure) = write_meta_with_outcome(
            &self.wal_dir,
            WalMeta {
                replay_floor: lsn,
                durable_end: state.flushed_lsn,
            },
        ) {
            state.poisoned = Some(wal_error(format!(
                "WAL manager is unavailable after replay-floor update failure: {}",
                failure.error.message
            )));
            return Err(failure.error);
        }
        #[cfg(test)]
        if let Some(message) = state.fail_next_parent_sync.take() {
            let error = DbError::io(message);
            state.poisoned = Some(error.clone());
            return Err(error);
        }
        let old_floor = state.replay_floor;
        state.replay_floor = lsn;
        recycle_segments(&self.wal_dir, old_floor, lsn)
    }

    fn flushed_lsn(&self) -> Lsn {
        self.state
            .lock()
            .map(|state| state.flushed_lsn)
            .unwrap_or(0)
    }

    fn retained_range(&self) -> Result<(Lsn, Lsn)> {
        let state = self.lock_state()?;
        Ok((state.replay_floor, state.flushed_lsn))
    }

    fn needs_clog_maintenance(&self) -> Result<bool> {
        let state = self.lock_state()?;
        Ok(state.clog.needs_maintenance(state.vacuum_floor))
    }

    fn bytes_after(&self, lsn: Lsn) -> Result<u64> {
        let state = self.lock_state()?;
        Ok(state
            .written_lsn
            .saturating_sub(lsn.max(state.replay_floor)))
    }

    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()> {
        {
            let state = self.lock_state()?;
            if state.clog_loaded_from_snapshot {
                return Ok(());
            }
        }
        let mut oldest_non_committed = None;
        for record in self.replay_from(0)? {
            let record = record?;
            if represents_transaction(&record) && !self.is_committed(record.txn_id) {
                oldest_non_committed = Some(
                    oldest_non_committed
                        .map_or(record.txn_id, |oldest: u64| oldest.min(record.txn_id)),
                );
            }
        }
        let floor = oldest_non_committed
            .map(|oldest| allocation_boundary.min(oldest))
            .unwrap_or(allocation_boundary);
        self.lock_state()?.clog.set_committed_floor(floor);
        Ok(())
    }

    fn resolve_in_flight_as_aborted(&self, writer_xids: &HashSet<u64>) -> Result<()> {
        let mut state = self.lock_state()?;
        for &xid in writer_xids {
            if state.clog.status(xid) == TxnStatus::InProgress {
                state.clog.set_aborted(xid);
            }
        }
        Ok(())
    }

    fn resolve_all_in_flight_as_aborted(&self) -> Result<()> {
        self.lock_state()?.clog.resolve_all_in_progress_as_aborted();
        Ok(())
    }

    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()> {
        let mut state = self.lock_state()?;
        state.vacuum_floor = state.vacuum_floor.max(boundary);
        Ok(())
    }

    fn checkpoint_clog(
        &self,
        proposed_replay_floor: Lsn,
        captured_active: &[TxnId],
        allocation_boundary: TxnId,
    ) -> Result<()> {
        self.flush()?;
        let snapshot = {
            let state = self.lock_state()?;
            if proposed_replay_floor < state.replay_floor
                || proposed_replay_floor > state.flushed_lsn
            {
                return Err(wal_error(
                    "proposed CLOG replay floor is outside the durable WAL range",
                ));
            }
            state.clog.live_snapshot(
                state.flushed_lsn,
                proposed_replay_floor,
                state.vacuum_floor,
                captured_active,
                allocation_boundary,
            )
        };
        write_clog_file(&self.clog_path(), &snapshot)?;
        let mut state = self.lock_state()?;
        state.clog.prune_to(snapshot.committed_floor);
        state.recycle_boundary = Some(snapshot.authorized_replay_floor);
        Ok(())
    }
}

impl TxnStatusView for FileWalManager {
    fn status(&self, xid: TxnId) -> TxnStatus {
        self.state
            .lock()
            .map(|state| state.clog.status(xid))
            .unwrap_or(TxnStatus::InProgress)
    }
}

struct WalReplay {
    reader: SegmentReader,
    threshold: Lsn,
    done: bool,
}

impl Iterator for WalReplay {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let replay_from = self.reader.position();
            match read_frame(&mut self.reader) {
                Ok(Some(_record)) if replay_from < self.threshold => {}
                Ok(Some(record)) => {
                    return Some(Ok(WalEntry {
                        replay_from,
                        record,
                    }));
                }
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(err) => {
                    self.done = true;
                    return Some(Err(err));
                }
            }
        }
    }
}

fn scan_stream(
    dir: &Path,
    start: Lsn,
    end: Lsn,
    mut visit: impl FnMut(&WalRecord) -> Result<()>,
) -> Result<()> {
    let mut reader = SegmentReader::new(dir.to_path_buf(), start, end);
    while reader.remaining() > 0 {
        match read_frame(&mut reader) {
            Ok(Some(record)) => visit(&record)?,
            Ok(None) => return Err(wal_error("durable WAL record is incomplete")),
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn read_frame(reader: &mut SegmentReader) -> Result<Option<WalRecord>> {
    if reader.remaining() == 0 {
        return Ok(None);
    }
    let start = reader.position();
    let Some(header) = reader.read_exact_vec(HEADER_LEN)? else {
        return Ok(None);
    };
    let frame_len = frame_len_from_header(&header)?;
    let rest_len = frame_len
        .checked_sub(HEADER_LEN)
        .ok_or_else(|| wal_error("WAL frame length is shorter than its header"))?;
    let Some(rest) = reader.read_exact_vec(rest_len)? else {
        return Ok(None);
    };
    let mut bytes = header;
    bytes
        .try_reserve_exact(rest.len())
        .map_err(|_| wal_error("cannot allocate complete WAL frame"))?;
    bytes.extend_from_slice(&rest);
    let record = decode_record(&bytes)?;
    let expected_end = start
        .checked_add(
            u64::try_from(frame_len).map_err(|_| wal_error("WAL frame length does not fit u64"))?,
        )
        .ok_or_else(|| wal_error("WAL frame end overflows"))?;
    if record.lsn != expected_end || reader.position() != expected_end {
        return Err(wal_error("stored WAL LSN does not match its byte position"));
    }
    Ok(Some(record))
}

fn represents_transaction(record: &WalRecord) -> bool {
    record.txn_id != 0 && !matches!(record.kind, WalRecordKind::Checkpoint { .. })
}

fn fold_status(clog: &mut Clog, record: &WalRecord) {
    match &record.kind {
        WalRecordKind::Commit => clog.set_committed(record.txn_id),
        WalRecordKind::CommitWithSubxids { subxids } => {
            clog.set_committed(record.txn_id);
            for &subxid in subxids {
                clog.set_committed(subxid);
            }
        }
        WalRecordKind::Abort => clog.set_aborted(record.txn_id),
        _ if represents_transaction(record)
            && clog.status(record.txn_id) == TxnStatus::InProgress =>
        {
            clog.set_in_progress(record.txn_id);
        }
        _ => {}
    }
}

fn load_clog_snapshot(path: &Path) -> Result<Option<ClogSnapshot>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(DbError::io(format!(
                "failed to read CLOG file {}: {err}",
                path.display()
            )));
        }
    };
    let length = file
        .metadata()
        .map_err(|err| DbError::io(format!("failed to stat CLOG file: {err}")))?
        .len();
    let maximum = u64::try_from(MAX_CLOG_FILE_BYTES)
        .map_err(|_| wal_error("CLOG file limit does not fit u64"))?;
    if length > maximum {
        return Err(wal_error("CLOG file exceeds the 64 MiB payload limit"));
    }
    let capacity =
        usize::try_from(length).map_err(|_| wal_error("CLOG file length does not fit usize"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| wal_error("cannot allocate CLOG file buffer"))?;
    let read_limit = maximum
        .checked_add(1)
        .ok_or_else(|| wal_error("CLOG file read limit overflows"))?;
    (&mut file)
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|err| DbError::io(format!("failed to read CLOG file: {err}")))?;
    if bytes.len() > MAX_CLOG_FILE_BYTES {
        return Err(wal_error("CLOG file exceeds the 64 MiB payload limit"));
    }
    decode_clog(&bytes).map(Some)
}

fn write_clog_file(path: &Path, snapshot: &ClogSnapshot) -> Result<()> {
    let bytes = encode_clog(snapshot)?;
    let tmp = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|err| {
                DbError::io(format!("failed to open CLOG file {}: {err}", tmp.display()))
            })?;
        file.write_all(&bytes).map_err(|err| {
            DbError::io(format!(
                "failed to write CLOG file {}: {err}",
                tmp.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            DbError::io(format!(
                "failed to fsync CLOG file {}: {err}",
                tmp.display()
            ))
        })?;
    }
    fs::rename(&tmp, path).map_err(|err| {
        DbError::io(format!(
            "failed to replace CLOG file {} with {}: {err}",
            path.display(),
            tmp.display()
        ))
    })?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn wal_error(message: impl Into<String>) -> DbError {
    DbError::wal(common::SqlState::InternalError, message)
}

#[cfg(test)]
impl FileWalManager {
    pub(crate) fn fail_next_flush_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_flush = Some(message.into());
    }

    pub(crate) fn fail_next_post_write_seek_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_post_write_seek = Some(message.into());
    }

    pub(crate) fn fail_next_parent_sync_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_parent_sync = Some(message.into());
    }

    pub(crate) fn fail_next_durable_end_sync_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_durable_end_sync = Some(message.into());
    }

    pub(crate) fn flushed_lsn_result_for_test(&self) -> Result<Lsn> {
        Ok(self.lock_state()?.flushed_lsn)
    }
}
