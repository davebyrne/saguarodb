#![cfg_attr(
    not(test),
    deny(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

//! Query-local spill files, memory accounting, rewindable tapes, and external sort.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use common::{
    ArrayDimension, DataType, DbError, Decimal, ExecRow, Key, MAX_ARRAY_DIMENSIONS, QueryCancel,
    Result, Row, RowId, RowIdentity, SqlArray, Value,
};

const MAGIC: &[u8; 4] = b"SGSP";
const VERSION: u32 = 1;
pub const DEFAULT_WORK_MEM_BYTES: u64 = 4 * 1024 * 1024;
pub const MIN_WORK_MEM_BYTES: u64 = 4 * 1024;

#[derive(Debug, Default)]
pub struct SpillStats {
    peak_reserved_bytes: AtomicU64,
    files_created: AtomicU64,
    bytes_written: AtomicU64,
}

impl SpillStats {
    pub fn peak_reserved_bytes(&self) -> u64 {
        self.peak_reserved_bytes.load(AtomicOrdering::Relaxed)
    }

    pub fn files_created(&self) -> u64 {
        self.files_created.load(AtomicOrdering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(AtomicOrdering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct SpillConfig {
    work_mem_bytes: u64,
    temp_dir: Arc<PathBuf>,
    pub stats: Arc<SpillStats>,
}

impl SpillConfig {
    pub fn new(work_mem_bytes: u64, temp_dir: PathBuf) -> Self {
        Self {
            work_mem_bytes: work_mem_bytes.max(MIN_WORK_MEM_BYTES),
            temp_dir: Arc::new(temp_dir),
            stats: Arc::new(SpillStats::default()),
        }
    }

    pub fn for_operator(&self, cancel: Arc<QueryCancel>) -> SpillContext {
        SpillContext {
            inner: Arc::new(SpillContextInner {
                limit: self.work_mem_bytes.max(MIN_WORK_MEM_BYTES),
                reserved: Mutex::new(0),
                temp_dir: Arc::clone(&self.temp_dir),
                cancel,
                stats: Arc::clone(&self.stats),
            }),
        }
    }

    pub fn work_mem_bytes(&self) -> u64 {
        self.work_mem_bytes
    }

    pub fn temp_dir(&self) -> &std::path::Path {
        self.temp_dir.as_ref()
    }
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self::new(DEFAULT_WORK_MEM_BYTES, std::env::temp_dir())
    }
}

#[derive(Clone, Debug)]
pub struct SpillContext {
    inner: Arc<SpillContextInner>,
}

#[derive(Debug)]
struct SpillContextInner {
    limit: u64,
    reserved: Mutex<u64>,
    temp_dir: Arc<PathBuf>,
    cancel: Arc<QueryCancel>,
    stats: Arc<SpillStats>,
}

impl SpillContext {
    pub fn limit(&self) -> u64 {
        self.inner.limit
    }

    pub fn reserved_bytes(&self) -> u64 {
        *self
            .inner
            .reserved
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn check_canceled(&self) -> Result<()> {
        self.inner.cancel.check()
    }

    fn try_reserve(&self, bytes: u64) -> bool {
        let mut reserved = self
            .inner
            .reserved
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let Some(next) = reserved.checked_add(bytes) else {
            return false;
        };
        if next > self.inner.limit {
            return false;
        }
        *reserved = next;
        self.inner
            .stats
            .peak_reserved_bytes
            .fetch_max(next, AtomicOrdering::Relaxed);
        true
    }

    fn release(&self, bytes: u64) {
        let mut reserved = self
            .inner
            .reserved
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        *reserved = reserved.saturating_sub(bytes);
    }

    pub fn reserve(&self, bytes: u64) -> Option<Reservation> {
        self.try_reserve(bytes).then(|| Reservation {
            ctx: self.clone(),
            bytes,
        })
    }

    fn create_file(&self) -> Result<File> {
        self.check_canceled()?;
        let mut file = tempfile::tempfile_in(self.inner.temp_dir.as_ref()).map_err(io_error)?;
        file.write_all(MAGIC).map_err(io_error)?;
        file.write_all(&VERSION.to_le_bytes()).map_err(io_error)?;
        self.inner
            .stats
            .files_created
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.inner
            .stats
            .bytes_written
            .fetch_add(8, AtomicOrdering::Relaxed);
        Ok(file)
    }

    fn record_write(&self, bytes: u64) {
        self.inner
            .stats
            .bytes_written
            .fetch_add(bytes, AtomicOrdering::Relaxed);
    }
}

#[must_use = "dropping the reservation releases its memory charge"]
pub struct Reservation {
    ctx: SpillContext,
    bytes: u64,
}

impl Reservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Extends this reservation without allocating bookkeeping proportional to
    /// the number of retained values.
    pub fn try_grow(&mut self, bytes: u64) -> bool {
        if self.ctx.try_reserve(bytes) {
            self.bytes = self.bytes.saturating_add(bytes);
            true
        } else {
            false
        }
    }

    pub fn shrink(&mut self, bytes: u64) {
        let bytes = bytes.min(self.bytes);
        self.ctx.release(bytes);
        self.bytes -= bytes;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.ctx.release(self.bytes);
    }
}

fn io_error(err: impl std::fmt::Display) -> DbError {
    DbError::io(format!("spill I/O failed: {err}"))
}

pub trait RetainedSize {
    fn retained_size(&self) -> u64;
}

macro_rules! fixed_size {
    ($($ty:ty),* $(,)?) => {$(
        impl RetainedSize for $ty {
            fn retained_size(&self) -> u64 { size_of::<Self>() as u64 }
        }
    )*};
}

fixed_size!(
    u8, u16, u32, u64, usize, i32, i64, i128, bool, Decimal, RowId
);

impl RetainedSize for Value {
    fn retained_size(&self) -> u64 {
        size_of::<Self>() as u64
            + match self {
                Value::Text(value) => value.capacity() as u64,
                Value::Bytes(value) => value.capacity() as u64,
                _ => 0,
            }
    }
}

impl<T: RetainedSize> RetainedSize for Vec<T> {
    fn retained_size(&self) -> u64 {
        size_of::<Self>() as u64
            + self.capacity() as u64 * size_of::<T>() as u64
            + self
                .iter()
                .map(|value| value.retained_size().saturating_sub(size_of::<T>() as u64))
                .sum::<u64>()
    }
}

impl RetainedSize for Key {
    fn retained_size(&self) -> u64 {
        size_of::<Self>() as u64 + values_heap_size(&self.0)
    }
}

impl RetainedSize for Row {
    fn retained_size(&self) -> u64 {
        size_of::<Self>() as u64 + values_heap_size(&self.values)
    }
}

impl RetainedSize for RowIdentity {
    fn retained_size(&self) -> u64 {
        size_of::<Self>() as u64 + values_heap_size(&self.key.0)
    }
}

impl RetainedSize for ExecRow {
    fn retained_size(&self) -> u64 {
        size_of::<Self>() as u64
            + values_heap_size(&self.row.values)
            + self
                .identity
                .as_ref()
                .map_or(0, |identity| values_heap_size(&identity.key.0))
    }
}

fn values_heap_size(values: &Vec<Value>) -> u64 {
    values.capacity() as u64 * size_of::<Value>() as u64
        + values
            .iter()
            .map(|value| {
                value
                    .retained_size()
                    .saturating_sub(size_of::<Value>() as u64)
            })
            .sum::<u64>()
}

pub trait SpillRecord: RetainedSize + Sized {
    fn encoded_len(&self) -> Result<u64>;
    fn encode<W: Write>(&self, writer: &mut W) -> Result<()>;
    fn decode<R: Read>(reader: &mut R, payload_len: u64) -> Result<Self>;
}

impl SpillRecord for ExecRow {
    fn encoded_len(&self) -> Result<u64> {
        codec::exec_row_len(self)
    }

    fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
        codec::encode_exec_row(self, writer)
    }

    fn decode<R: Read>(reader: &mut R, _payload_len: u64) -> Result<Self> {
        codec::decode_exec_row(reader)
    }
}

impl SpillRecord for Value {
    fn encoded_len(&self) -> Result<u64> {
        codec::value_len(self)
    }
    fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
        codec::encode_value(self, writer)
    }
    fn decode<R: Read>(reader: &mut R, _payload_len: u64) -> Result<Self> {
        codec::decode_value(reader)
    }
}

impl SpillRecord for Row {
    fn encoded_len(&self) -> Result<u64> {
        codec::values_len(&self.values)
    }
    fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
        codec::encode_values(&self.values, writer)
    }
    fn decode<R: Read>(reader: &mut R, _payload_len: u64) -> Result<Self> {
        Ok(Row {
            values: codec::decode_values(reader)?,
        })
    }
}

fn write_record<T: SpillRecord>(file: &mut File, record: &T, ctx: &SpillContext) -> Result<()> {
    ctx.check_canceled()?;
    let len = record.encoded_len()?;
    file.write_all(&len.to_le_bytes()).map_err(io_error)?;
    let mut limited = CountingWriter {
        inner: file,
        written: 0,
    };
    record.encode(&mut limited)?;
    if limited.written != len {
        return Err(DbError::io(
            "spill codec wrote an unexpected payload length",
        ));
    }
    ctx.record_write(8 + len);
    Ok(())
}

fn read_record<T: SpillRecord>(file: &mut File, ctx: &SpillContext) -> Result<Option<T>> {
    ctx.check_canceled()?;
    let mut len = [0; 8];
    match file.read(&mut len[..1]).map_err(io_error)? {
        0 => return Ok(None),
        1 => file.read_exact(&mut len[1..]).map_err(io_error)?,
        _ => {
            return Err(io_error(
                "one-byte spill read returned an invalid byte count",
            ));
        }
    }
    let payload_len = u64::from_le_bytes(len);
    let mut take = file.take(payload_len);
    let value = T::decode(&mut take, payload_len)?;
    if take.limit() != 0 {
        return Err(io_error("spill codec did not consume its complete payload"));
    }
    Ok(Some(value))
}

struct AccountedRecord<T> {
    value: T,
    charged: u64,
    ctx: SpillContext,
}

impl<T> Drop for AccountedRecord<T> {
    fn drop(&mut self) {
        self.ctx.release(self.charged);
    }
}

fn read_accounted<T: SpillRecord>(
    file: &mut File,
    ctx: &SpillContext,
) -> Result<Option<AccountedRecord<T>>> {
    let Some(value) = read_record::<T>(file, ctx)? else {
        return Ok(None);
    };
    let size = value.retained_size();
    // A merge has at most two heads. If their combined size exceeds work_mem,
    // keep the uncharged head as the documented constant oversized-record
    // allowance rather than making progress impossible.
    let charged = if ctx.try_reserve(size) { size } else { 0 };
    Ok(Some(AccountedRecord {
        value,
        charged,
        ctx: ctx.clone(),
    }))
}

fn rewind_data(file: &mut File) -> Result<()> {
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut magic = [0; 4];
    let mut version = [0; 4];
    file.read_exact(&mut magic).map_err(io_error)?;
    file.read_exact(&mut version).map_err(io_error)?;
    if &magic != MAGIC || u32::from_le_bytes(version) != VERSION {
        return Err(io_error("invalid spill file header"));
    }
    Ok(())
}

struct CountingWriter<'a, W> {
    inner: &'a mut W,
    written: u64,
}

impl<W: Write> Write for CountingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let count = self.inner.write(buf)?;
        self.written += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

enum TapeStorage<T> {
    Memory { records: Vec<T>, charged: u64 },
    SharedMemory(Arc<SharedRecords<T>>),
    Disk(Arc<Mutex<File>>),
}

struct SharedRecords<T> {
    records: Vec<T>,
    ctx: SpillContext,
    charged: u64,
}

impl<T> Drop for SharedRecords<T> {
    fn drop(&mut self) {
        self.ctx.release(self.charged);
    }
}

fn reserve_vec_item<T>(
    ctx: &SpillContext,
    values: &mut Vec<T>,
    retained_size: u64,
    item_inline_size: usize,
) -> Result<Option<(u64, u64)>> {
    let heap_size = retained_size.saturating_sub(item_inline_size as u64);
    if values.len() < values.capacity() || size_of::<T>() == 0 {
        return Ok(ctx.try_reserve(heap_size).then_some((heap_size, 0)));
    }

    // Reserve the complete replacement allocation while the old allocation is
    // still charged. This makes the transient grow-and-swap peak obey the budget.
    let requested_capacity = values.len().saturating_add(1);
    let initial_charge =
        heap_size.saturating_add(requested_capacity.saturating_mul(size_of::<T>()) as u64);
    if !ctx.try_reserve(initial_charge) {
        return Ok(None);
    }
    let mut candidate = Vec::new();
    if let Err(err) = candidate.try_reserve_exact(requested_capacity) {
        ctx.release(initial_charge);
        return Err(DbError::internal(format!(
            "cannot reserve spill buffer: {err}"
        )));
    }
    let actual_capacity_bytes = candidate.capacity().saturating_mul(size_of::<T>()) as u64;
    let requested_capacity_bytes = requested_capacity.saturating_mul(size_of::<T>()) as u64;
    let extra = actual_capacity_bytes.saturating_sub(requested_capacity_bytes);
    if extra != 0 && !ctx.try_reserve(extra) {
        drop(candidate);
        ctx.release(initial_charge);
        return Ok(None);
    }
    let old_capacity_bytes = values.capacity().saturating_mul(size_of::<T>()) as u64;
    candidate.append(values);
    *values = candidate;
    ctx.release(old_capacity_bytes);
    Ok(Some((
        heap_size,
        actual_capacity_bytes.saturating_sub(old_capacity_bytes),
    )))
}

pub struct SpillTape<T: SpillRecord> {
    ctx: SpillContext,
    storage: TapeStorage<T>,
    finished: bool,
    failed: bool,
}

impl<T: SpillRecord> SpillTape<T> {
    pub fn new(ctx: SpillContext) -> Self {
        Self {
            ctx,
            storage: TapeStorage::Memory {
                records: Vec::new(),
                charged: 0,
            },
            finished: false,
            failed: false,
        }
    }

    pub fn disk_only(ctx: SpillContext) -> Result<Self> {
        let file = ctx.create_file()?;
        Ok(Self {
            ctx,
            storage: TapeStorage::Disk(Arc::new(Mutex::new(file))),
            finished: false,
            failed: false,
        })
    }

    pub fn push(&mut self, record: T) -> Result<()> {
        if self.finished {
            return Err(DbError::internal("cannot append to a finished spill tape"));
        }
        if self.failed {
            return Err(DbError::internal(
                "cannot reuse a spill tape after a write failure",
            ));
        }
        self.ctx.check_canceled()?;
        let charge = match &mut self.storage {
            TapeStorage::Memory { records, .. } => {
                reserve_vec_item(&self.ctx, records, record.retained_size(), size_of::<T>())?
            }
            TapeStorage::SharedMemory(_) => {
                return Err(DbError::internal("finished spill tape is immutable"));
            }
            TapeStorage::Disk(_) => Some((0, 0)),
        };
        if let TapeStorage::Memory { records, charged } = &mut self.storage
            && let Some((heap, capacity)) = charge
        {
            let size = heap.saturating_add(capacity);
            records.push(record);
            *charged += size;
            return Ok(());
        }
        self.migrate_to_disk()?;
        let TapeStorage::Disk(file) = &mut self.storage else {
            return Err(DbError::internal(
                "spill tape migration did not produce disk storage",
            ));
        };
        let mut file = file.lock().unwrap_or_else(|p| p.into_inner());
        let result = write_record(&mut file, &record, &self.ctx);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn migrate_to_disk(&mut self) -> Result<()> {
        if matches!(self.storage, TapeStorage::Disk(_)) {
            return Ok(());
        }
        let mut file = self.ctx.create_file()?;
        let records: &[T] = match &self.storage {
            TapeStorage::Memory { records, .. } => records,
            TapeStorage::SharedMemory(records) => &records.records,
            TapeStorage::Disk(_) => {
                return Err(DbError::internal(
                    "disk spill tape unexpectedly required migration",
                ));
            }
        };
        // Do not take ownership of the in-memory state until every write has
        // succeeded. On error the tape remains intact and its reservation stays
        // owned by Drop.
        for record in records {
            write_record(&mut file, record, &self.ctx)?;
        }
        let storage = std::mem::replace(
            &mut self.storage,
            TapeStorage::Disk(Arc::new(Mutex::new(file))),
        );
        let charged = match storage {
            TapeStorage::Memory { charged, .. } => charged,
            TapeStorage::SharedMemory(records) => {
                drop(records);
                0
            }
            TapeStorage::Disk(_) => {
                return Err(DbError::internal(
                    "spill tape migration replaced disk storage",
                ));
            }
        };
        self.ctx.release(charged);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if self.failed {
            return Err(DbError::internal(
                "cannot finish a spill tape after a write failure",
            ));
        }
        self.ctx.check_canceled()?;
        self.finished = true;
        Ok(())
    }

    pub fn reader(&mut self) -> Result<SpillTapeReader<T>>
    where
        T: Clone,
    {
        if !self.finished {
            return Err(DbError::internal("cannot read an unfinished spill tape"));
        }
        if self.failed {
            return Err(DbError::internal(
                "cannot read a spill tape after a write failure",
            ));
        }
        if let TapeStorage::Memory { .. } = self.storage {
            let replaced = std::mem::replace(
                &mut self.storage,
                TapeStorage::Memory {
                    records: Vec::new(),
                    charged: 0,
                },
            );
            let (records, charged) = match replaced {
                TapeStorage::Memory { records, charged } => (records, charged),
                other => {
                    self.storage = other;
                    return Err(DbError::internal(
                        "spill tape storage changed while creating a reader",
                    ));
                }
            };
            self.storage = TapeStorage::SharedMemory(Arc::new(SharedRecords {
                records,
                ctx: self.ctx.clone(),
                charged,
            }));
        }
        match &self.storage {
            TapeStorage::SharedMemory(records) => Ok(SpillTapeReader {
                storage: TapeReaderStorage::Memory {
                    records: Arc::clone(records),
                    index: 0,
                },
                ctx: self.ctx.clone(),
            }),
            TapeStorage::Disk(file) => {
                let mut file_guard = file.lock().unwrap_or_else(|p| p.into_inner());
                rewind_data(&mut file_guard)?;
                drop(file_guard);
                Ok(SpillTapeReader {
                    storage: TapeReaderStorage::Disk {
                        file: Arc::clone(file),
                        position: 8,
                        marker: PhantomData,
                    },
                    ctx: self.ctx.clone(),
                })
            }
            TapeStorage::Memory { .. } => Err(DbError::internal(
                "finished spill tape remained in mutable memory storage",
            )),
        }
    }
}

impl<T: SpillRecord> Drop for SpillTape<T> {
    fn drop(&mut self) {
        if let TapeStorage::Memory { charged, .. } = &self.storage {
            self.ctx.release(*charged);
        }
    }
}

pub struct SpillTapeReader<T: SpillRecord> {
    storage: TapeReaderStorage<T>,
    ctx: SpillContext,
}

impl<T: SpillRecord + Clone> Clone for SpillTapeReader<T> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            ctx: self.ctx.clone(),
        }
    }
}

#[derive(Clone)]
enum TapeReaderStorage<T> {
    Memory {
        records: Arc<SharedRecords<T>>,
        index: usize,
    },
    Disk {
        file: Arc<Mutex<File>>,
        position: u64,
        marker: PhantomData<T>,
    },
}

impl<T: SpillRecord + Clone> SpillTapeReader<T> {
    pub fn next_record(&mut self) -> Result<Option<T>> {
        self.ctx.check_canceled()?;
        match &mut self.storage {
            TapeReaderStorage::Memory { records, index } => {
                let record = records.records.get(*index).cloned();
                *index += usize::from(record.is_some());
                Ok(record)
            }
            TapeReaderStorage::Disk { file, position, .. } => {
                let mut file = file.lock().unwrap_or_else(|p| p.into_inner());
                file.seek(SeekFrom::Start(*position)).map_err(io_error)?;
                let record = read_record(&mut file, &self.ctx)?;
                *position = file.stream_position().map_err(io_error)?;
                Ok(record)
            }
        }
    }
}

struct Run<T> {
    file: File,
    marker: PhantomData<T>,
}

struct Buffered<T> {
    value: T,
    heap_charge: u64,
    ordinal: u64,
}

fn sort_buffer_cancelable<T, C>(
    ctx: &SpillContext,
    compare: &C,
    buffer: &mut [Buffered<T>],
) -> Result<()>
where
    C: Fn(&T, &T) -> Ordering,
{
    use std::cell::Cell;

    const POLL_EVERY_COMPARISONS: usize = 256;
    let comparisons = Cell::new(0usize);
    let canceled = Cell::new(false);
    buffer.sort_unstable_by(|left, right| {
        if canceled.get() {
            return Ordering::Equal;
        }
        let count = comparisons.get();
        comparisons.set(count.wrapping_add(1));
        if count.is_multiple_of(POLL_EVERY_COMPARISONS) && ctx.check_canceled().is_err() {
            canceled.set(true);
            return Ordering::Equal;
        }
        compare(&left.value, &right.value).then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    ctx.check_canceled()
}

pub struct ExternalSorter<T, C>
where
    T: SpillRecord,
    C: Fn(&T, &T) -> Ordering,
{
    ctx: SpillContext,
    compare: C,
    buffer: Vec<Buffered<T>>,
    charged: u64,
    levels: Vec<Option<Run<T>>>,
    levels_charged: u64,
    failed: bool,
    next_ordinal: u64,
}

impl<T, C> ExternalSorter<T, C>
where
    T: SpillRecord,
    C: Fn(&T, &T) -> Ordering,
{
    pub fn new(ctx: SpillContext, compare: C) -> Self {
        Self {
            ctx,
            compare,
            buffer: Vec::new(),
            charged: 0,
            levels: Vec::new(),
            levels_charged: 0,
            failed: false,
            next_ordinal: 0,
        }
    }

    pub fn push(&mut self, record: T) -> Result<()> {
        if self.failed {
            return Err(DbError::internal(
                "cannot reuse an external sorter after a spill failure",
            ));
        }
        self.ctx.check_canceled()?;
        if let Some((heap_charge, capacity_charge)) = reserve_vec_item(
            &self.ctx,
            &mut self.buffer,
            record.retained_size(),
            size_of::<T>(),
        )? {
            let size = heap_charge.saturating_add(capacity_charge);
            self.charged += size;
            let buffered = self.buffered(record, heap_charge)?;
            self.buffer.push(buffered);
            return Ok(());
        }
        if !self.buffer.is_empty() {
            self.flush_run()?;
        } else {
            // `prepare_vec_charges` measured an actual allocation before the
            // budget rejected it. Drop that empty allocation so the retry must
            // account its capacity again.
            self.buffer = Vec::new();
        }
        if let Some((heap_charge, capacity_charge)) = reserve_vec_item(
            &self.ctx,
            &mut self.buffer,
            record.retained_size(),
            size_of::<T>(),
        )? {
            let size = heap_charge.saturating_add(capacity_charge);
            self.charged += size;
            let buffered = self.buffered(record, heap_charge)?;
            self.buffer.push(buffered);
            return Ok(());
        }
        let buffered = self.buffered(record, 0)?;
        self.buffer.push(buffered);
        self.flush_run()?;
        Ok(())
    }

    fn buffered(&mut self, value: T, heap_charge: u64) -> Result<Buffered<T>> {
        let ordinal = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| DbError::internal("external sorter input ordinal overflow"))?;
        Ok(Buffered {
            value,
            heap_charge,
            ordinal,
        })
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ctx.check_canceled()?;
        sort_buffer_cancelable(&self.ctx, &self.compare, &mut self.buffer)?;
        let mut file = self.ctx.create_file()?;
        for record in &self.buffer {
            write_record(&mut file, &record.value, &self.ctx)?;
        }
        // Drop the backing allocation before releasing its capacity charge.
        self.buffer = Vec::new();
        self.ctx.release(self.charged);
        self.charged = 0;
        let result = self.insert_run(
            Run {
                file,
                marker: PhantomData,
            },
            0,
        );
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn insert_run(&mut self, mut run: Run<T>, mut level: usize) -> Result<()> {
        loop {
            if self.levels.len() <= level {
                while self.levels.len() <= level {
                    let Some((heap, capacity)) = reserve_vec_item(
                        &self.ctx,
                        &mut self.levels,
                        size_of::<Option<Run<T>>>() as u64,
                        size_of::<Option<Run<T>>>(),
                    )?
                    else {
                        return Err(DbError::internal(
                            "work_mem is too small for external-sort run metadata",
                        ));
                    };
                    self.levels_charged = self
                        .levels_charged
                        .saturating_add(heap.saturating_add(capacity));
                    self.levels.push(None);
                }
            }
            match self.levels[level].take() {
                None => {
                    self.levels[level] = Some(run);
                    return Ok(());
                }
                Some(other) => {
                    run = self.merge_runs(other, run)?;
                    level += 1;
                }
            }
        }
    }

    fn merge_runs(&self, mut left: Run<T>, mut right: Run<T>) -> Result<Run<T>> {
        rewind_data(&mut left.file)?;
        rewind_data(&mut right.file)?;
        let mut output = self.ctx.create_file()?;
        let mut l = read_accounted::<T>(&mut left.file, &self.ctx)?;
        let mut r = read_accounted::<T>(&mut right.file, &self.ctx)?;
        while l.is_some() || r.is_some() {
            self.ctx.check_canceled()?;
            let take_left = match (&l, &r) {
                (Some(a), Some(b)) => (self.compare)(&a.value, &b.value) != Ordering::Greater,
                (Some(_), None) => true,
                _ => false,
            };
            if take_left {
                let Some(emitted) = l.take() else {
                    return Err(DbError::internal(
                        "external sort merge selected an exhausted left run",
                    ));
                };
                write_record(&mut output, &emitted.value, &self.ctx)?;
                drop(emitted);
                l = read_accounted(&mut left.file, &self.ctx)?;
            } else {
                let Some(emitted) = r.take() else {
                    return Err(DbError::internal(
                        "external sort merge selected an exhausted right run",
                    ));
                };
                write_record(&mut output, &emitted.value, &self.ctx)?;
                drop(emitted);
                r = read_accounted(&mut right.file, &self.ctx)?;
            }
        }
        Ok(Run {
            file: output,
            marker: PhantomData,
        })
    }

    pub fn finish(mut self) -> Result<SortedStream<T>> {
        if self.failed {
            return Err(DbError::internal(
                "cannot finish an external sorter after a spill failure",
            ));
        }
        if self.levels.iter().all(Option::is_none) {
            self.ctx.check_canceled()?;
            sort_buffer_cancelable(&self.ctx, &self.compare, &mut self.buffer)?;
            let records = std::mem::take(&mut self.buffer).into_iter();
            let charged = std::mem::take(&mut self.charged);
            return Ok(SortedStream {
                storage: SortedStorage::Memory {
                    records,
                    charged,
                    ctx: self.ctx.clone(),
                },
            });
        }
        self.flush_run()?;
        let mut final_run = None;
        // Higher levels contain older input runs. Keep them on the left of
        // equal-key merges so stable order remains the original input order.
        for run in std::mem::take(&mut self.levels).into_iter().rev().flatten() {
            final_run = Some(match final_run {
                None => run,
                Some(current) => self.merge_runs(current, run)?,
            });
        }
        let mut file = final_run
            .ok_or_else(|| DbError::internal("flushed external sorter has no merge run"))?
            .file;
        rewind_data(&mut file)?;
        Ok(SortedStream {
            storage: SortedStorage::Disk {
                file,
                ctx: self.ctx.clone(),
                marker: PhantomData,
            },
        })
    }
}

impl<T, C> Drop for ExternalSorter<T, C>
where
    T: SpillRecord,
    C: Fn(&T, &T) -> Ordering,
{
    fn drop(&mut self) {
        self.ctx.release(self.charged);
        self.ctx.release(self.levels_charged);
    }
}

pub struct SortedStream<T: SpillRecord> {
    storage: SortedStorage<T>,
}

enum SortedStorage<T: SpillRecord> {
    Memory {
        records: std::vec::IntoIter<Buffered<T>>,
        charged: u64,
        ctx: SpillContext,
    },
    Disk {
        file: File,
        ctx: SpillContext,
        marker: PhantomData<T>,
    },
}

impl<T: SpillRecord> SortedStream<T> {
    pub fn next_record(&mut self) -> Result<Option<T>> {
        let ctx = match &self.storage {
            SortedStorage::Memory { ctx, .. } | SortedStorage::Disk { ctx, .. } => ctx,
        };
        ctx.check_canceled()?;
        match &mut self.storage {
            SortedStorage::Memory {
                records,
                charged,
                ctx,
            } => {
                let value = records.next();
                if let Some(value) = &value {
                    let size = value.heap_charge.min(*charged);
                    *charged -= size;
                    ctx.release(size);
                }
                Ok(value.map(|value| value.value))
            }
            SortedStorage::Disk { file, ctx, .. } => read_record(file, ctx),
        }
    }
}

impl<T: SpillRecord> Drop for SortedStream<T> {
    fn drop(&mut self) {
        if let SortedStorage::Memory { charged, ctx, .. } = &self.storage {
            ctx.release(*charged);
        }
    }
}

pub mod codec {
    use super::*;

    pub fn values_len(values: &[Value]) -> Result<u64> {
        let mut len = 8u64;
        for value in values {
            len = len
                .checked_add(value_len(value)?)
                .ok_or_else(|| io_error("spill value length overflow"))?;
        }
        Ok(len)
    }

    pub fn encode_values<W: Write>(values: &[Value], writer: &mut W) -> Result<()> {
        writer
            .write_all(&(values.len() as u64).to_le_bytes())
            .map_err(io_error)?;
        for value in values {
            encode_value(value, writer)?;
        }
        Ok(())
    }

    pub fn decode_values<R: Read>(reader: &mut R) -> Result<Vec<Value>> {
        let count = read_u64(reader)?;
        let count =
            usize::try_from(count).map_err(|_| io_error("spill value count overflows usize"))?;
        let mut values = Vec::new();
        // Grow only after each value has actually decoded. A corrupt count must
        // fail on the bounded record reader rather than preallocating attacker-
        // controlled memory.
        for _ in 0..count {
            values.push(decode_value(reader)?);
        }
        Ok(values)
    }

    pub fn exec_row_len(row: &ExecRow) -> Result<u64> {
        let mut len = values_len(&row.row.values)?
            .checked_add(1)
            .ok_or_else(|| io_error("spill row length overflow"))?;
        if let Some(identity) = &row.identity {
            len = len
                .checked_add(14)
                .and_then(|v| v.checked_add(values_len(&identity.key.0).ok()?))
                .ok_or_else(|| io_error("spill row length overflow"))?;
        }
        Ok(len)
    }

    pub fn encode_exec_row<W: Write>(row: &ExecRow, writer: &mut W) -> Result<()> {
        encode_values(&row.row.values, writer)?;
        match &row.identity {
            None => writer.write_all(&[0]).map_err(io_error)?,
            Some(identity) => {
                writer.write_all(&[1]).map_err(io_error)?;
                writer
                    .write_all(&identity.row_id.page_num.to_le_bytes())
                    .map_err(io_error)?;
                writer
                    .write_all(&identity.row_id.slot_num.to_le_bytes())
                    .map_err(io_error)?;
                writer
                    .write_all(&identity.xmin.to_le_bytes())
                    .map_err(io_error)?;
                encode_values(&identity.key.0, writer)?;
            }
        }
        Ok(())
    }

    pub fn decode_exec_row<R: Read>(reader: &mut R) -> Result<ExecRow> {
        let values = decode_values(reader)?;
        let mut present = [0];
        reader.read_exact(&mut present).map_err(io_error)?;
        let identity = match present[0] {
            0 => None,
            1 => Some(RowIdentity {
                row_id: RowId {
                    page_num: read_u32(reader)?,
                    slot_num: read_u16(reader)?,
                },
                xmin: read_u64(reader)?,
                key: Key(decode_values(reader)?),
            }),
            _ => return Err(io_error("invalid spill row identity tag")),
        };
        Ok(ExecRow {
            row: Row { values },
            identity,
        })
    }

    pub fn value_len(value: &Value) -> Result<u64> {
        Ok(1 + match value {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Integer(_) | Value::Float(_) => 8,
            Value::Real(_) => 4,
            Value::Numeric(_) => 20,
            Value::Text(v) => 8 + v.len() as u64,
            Value::Bytes(v) => 8 + v.len() as u64,
            Value::Date(_) | Value::Timestamp(_) | Value::Time(_) | Value::TimestampTz(_) => 8,
            Value::Interval(_) => 16,
            Value::Uuid(_) => 16,
            Value::Array(array) => array_len(array)?,
        })
    }

    pub fn encode_value<W: Write>(value: &Value, w: &mut W) -> Result<()> {
        let tag = match value {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Integer(_) => 2,
            Value::Float(_) => 3,
            Value::Real(_) => 4,
            Value::Numeric(_) => 5,
            Value::Text(_) => 6,
            Value::Date(_) => 7,
            Value::Timestamp(_) => 8,
            Value::Time(_) => 9,
            Value::TimestampTz(_) => 10,
            Value::Interval(_) => 11,
            Value::Bytes(_) => 12,
            Value::Uuid(_) => 13,
            Value::Array(_) => 14,
        };
        w.write_all(&[tag]).map_err(io_error)?;
        match value {
            Value::Null => {}
            Value::Boolean(v) => w.write_all(&[*v as u8]).map_err(io_error)?,
            Value::Integer(v)
            | Value::Date(v)
            | Value::Timestamp(v)
            | Value::Time(v)
            | Value::TimestampTz(v) => w.write_all(&v.to_le_bytes()).map_err(io_error)?,
            Value::Float(v) => w
                .write_all(&v.0.to_bits().to_le_bytes())
                .map_err(io_error)?,
            Value::Real(v) => w
                .write_all(&v.0.to_bits().to_le_bytes())
                .map_err(io_error)?,
            Value::Numeric(v) => {
                w.write_all(&v.mantissa().to_le_bytes()).map_err(io_error)?;
                w.write_all(&v.scale().to_le_bytes()).map_err(io_error)?;
            }
            Value::Text(v) => write_bytes(v.as_bytes(), w)?,
            Value::Bytes(v) => write_bytes(v, w)?,
            Value::Interval(v) => {
                w.write_all(&v.months.to_le_bytes()).map_err(io_error)?;
                w.write_all(&v.days.to_le_bytes()).map_err(io_error)?;
                w.write_all(&v.micros.to_le_bytes()).map_err(io_error)?;
            }
            Value::Uuid(v) => w.write_all(v).map_err(io_error)?,
            Value::Array(array) => {
                encode_data_type(array.element_type(), w)?;
                let dimensions = u32::try_from(array.dimensions().len())
                    .map_err(|_| io_error("spill array has too many dimensions"))?;
                w.write_all(&dimensions.to_le_bytes()).map_err(io_error)?;
                for dimension in array.dimensions() {
                    w.write_all(&dimension.len().to_le_bytes())
                        .map_err(io_error)?;
                    w.write_all(&dimension.lower_bound().to_le_bytes())
                        .map_err(io_error)?;
                }
                encode_values(array.elements(), w)?;
            }
        }
        Ok(())
    }

    pub fn decode_value<R: Read>(r: &mut R) -> Result<Value> {
        let mut tag = [0];
        r.read_exact(&mut tag).map_err(io_error)?;
        Ok(match tag[0] {
            0 => Value::Null,
            1 => {
                let mut b = [0];
                r.read_exact(&mut b).map_err(io_error)?;
                match b[0] {
                    0 => Value::Boolean(false),
                    1 => Value::Boolean(true),
                    _ => return Err(io_error("invalid spill boolean")),
                }
            }
            2 => Value::Integer(read_i64(r)?),
            3 => Value::Float(f64::from_bits(read_u64(r)?).into()),
            4 => Value::Real(f32::from_bits(read_u32(r)?).into()),
            5 => {
                let mantissa = read_i128(r)?;
                let scale = read_u32(r)?;
                if scale > 28 {
                    return Err(io_error("invalid spill numeric scale"));
                }
                Value::Numeric(Decimal::from_i128_with_scale(mantissa, scale))
            }
            6 => Value::Text(String::from_utf8(read_bytes(r)?).map_err(io_error)?),
            7 => Value::Date(read_i64(r)?),
            8 => Value::Timestamp(read_i64(r)?),
            9 => Value::Time(read_i64(r)?),
            10 => Value::TimestampTz(read_i64(r)?),
            11 => Value::Interval(common::Interval::new(
                read_i32(r)?,
                read_i32(r)?,
                read_i64(r)?,
            )),
            12 => Value::Bytes(read_bytes(r)?),
            13 => {
                let mut v = [0; 16];
                r.read_exact(&mut v).map_err(io_error)?;
                Value::Uuid(v)
            }
            14 => {
                let element_type = decode_data_type(r)?;
                let dimension_count = usize::try_from(read_u32(r)?)
                    .map_err(|_| io_error("spill array dimension count overflows usize"))?;
                if dimension_count > MAX_ARRAY_DIMENSIONS {
                    return Err(io_error("spill array has too many dimensions"));
                }
                let mut dimensions = Vec::with_capacity(dimension_count);
                for _ in 0..dimension_count {
                    dimensions.push(ArrayDimension::new(read_u32(r)?, read_i32(r)?));
                }
                Value::Array(SqlArray::new(element_type, dimensions, decode_values(r)?)?)
            }
            _ => return Err(io_error("unknown spill value tag")),
        })
    }

    fn array_len(array: &SqlArray) -> Result<u64> {
        let dimensions = u64::try_from(array.dimensions().len())
            .map_err(|_| io_error("spill array dimension count overflow"))?;
        data_type_len(array.element_type())
            .checked_add(4)
            .and_then(|len| len.checked_add(dimensions.checked_mul(8)?))
            .and_then(|len| len.checked_add(values_len(array.elements()).ok()?))
            .ok_or_else(|| io_error("spill array length overflow"))
    }

    fn data_type_len(data_type: &DataType) -> u64 {
        if matches!(data_type, DataType::Numeric { .. }) {
            9
        } else {
            1
        }
    }

    fn encode_data_type<W: Write>(data_type: &DataType, w: &mut W) -> Result<()> {
        let tag = match data_type {
            DataType::Integer => 0,
            DataType::Text => 1,
            DataType::Boolean => 2,
            DataType::Date => 3,
            DataType::Timestamp => 4,
            DataType::Time => 5,
            DataType::TimestampTz => 6,
            DataType::Interval => 7,
            DataType::Bytea => 8,
            DataType::Uuid => 9,
            DataType::Double => 10,
            DataType::Real => 11,
            DataType::Numeric { .. } => 12,
            DataType::Array(_) => return Err(io_error("nested spill array type")),
        };
        w.write_all(&[tag]).map_err(io_error)?;
        if let DataType::Numeric { precision, scale } = data_type {
            w.write_all(&precision.unwrap_or(u32::MAX).to_le_bytes())
                .map_err(io_error)?;
            w.write_all(&scale.to_le_bytes()).map_err(io_error)?;
        }
        Ok(())
    }

    fn decode_data_type<R: Read>(r: &mut R) -> Result<DataType> {
        let mut tag = [0];
        r.read_exact(&mut tag).map_err(io_error)?;
        Ok(match tag[0] {
            0 => DataType::Integer,
            1 => DataType::Text,
            2 => DataType::Boolean,
            3 => DataType::Date,
            4 => DataType::Timestamp,
            5 => DataType::Time,
            6 => DataType::TimestampTz,
            7 => DataType::Interval,
            8 => DataType::Bytea,
            9 => DataType::Uuid,
            10 => DataType::Double,
            11 => DataType::Real,
            12 => {
                let precision = match read_u32(r)? {
                    u32::MAX => None,
                    precision => Some(precision),
                };
                DataType::Numeric {
                    precision,
                    scale: read_u32(r)?,
                }
            }
            _ => return Err(io_error("unknown spill data type tag")),
        })
    }

    fn write_bytes<W: Write>(v: &[u8], w: &mut W) -> Result<()> {
        w.write_all(&(v.len() as u64).to_le_bytes())
            .map_err(io_error)?;
        w.write_all(v).map_err(io_error)?;
        Ok(())
    }
    fn read_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>> {
        let len = usize::try_from(read_u64(r)?)
            .map_err(|_| io_error("spill byte length overflows usize"))?;
        let mut v = Vec::new();
        let mut remaining = len;
        let mut chunk = [0; 8192];
        while remaining != 0 {
            let take = remaining.min(chunk.len());
            r.read_exact(&mut chunk[..take]).map_err(io_error)?;
            v.try_reserve(take)
                .map_err(|_| io_error("spill byte length is too large"))?;
            v.extend_from_slice(&chunk[..take]);
            remaining -= take;
        }
        Ok(v)
    }
    fn read_array<const N: usize, R: Read>(r: &mut R) -> Result<[u8; N]> {
        let mut v = [0; N];
        r.read_exact(&mut v).map_err(io_error)?;
        Ok(v)
    }
    fn read_u16<R: Read>(r: &mut R) -> Result<u16> {
        Ok(u16::from_le_bytes(read_array(r)?))
    }
    fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
        Ok(u32::from_le_bytes(read_array(r)?))
    }
    fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
        Ok(u64::from_le_bytes(read_array(r)?))
    }
    fn read_i32<R: Read>(r: &mut R) -> Result<i32> {
        Ok(i32::from_le_bytes(read_array(r)?))
    }
    fn read_i64<R: Read>(r: &mut R) -> Result<i64> {
        Ok(i64::from_le_bytes(read_array(r)?))
    }
    fn read_i128<R: Read>(r: &mut R) -> Result<i128> {
        Ok(i128::from_le_bytes(read_array(r)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::CancelReason;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Number(u64);
    impl RetainedSize for Number {
        fn retained_size(&self) -> u64 {
            16
        }
    }
    impl SpillRecord for Number {
        fn encoded_len(&self) -> Result<u64> {
            Ok(8)
        }
        fn encode<W: Write>(&self, w: &mut W) -> Result<()> {
            w.write_all(&self.0.to_le_bytes()).map_err(io_error)
        }
        fn decode<R: Read>(r: &mut R, _: u64) -> Result<Self> {
            let mut v = [0; 8];
            r.read_exact(&mut v).map_err(io_error)?;
            Ok(Self(u64::from_le_bytes(v)))
        }
    }

    #[test]
    fn tape_spills_and_rewinds() {
        let d = tempfile::tempdir().unwrap();
        let c = SpillConfig::new(MIN_WORK_MEM_BYTES, d.path().into());
        let ctx = c.for_operator(Arc::new(QueryCancel::new()));
        let mut t = SpillTape::new(ctx);
        for i in 0..1000 {
            t.push(Number(i)).unwrap();
        }
        t.finish().unwrap();
        let mut r = t.reader().unwrap();
        let mut got = Vec::new();
        while let Some(v) = r.next_record().unwrap() {
            got.push(v.0);
        }
        assert_eq!(got, (0..1000).collect::<Vec<_>>());
        assert!(c.stats.files_created() > 0);
    }

    #[test]
    fn tape_readers_keep_independent_positions() {
        let dir = tempfile::tempdir().unwrap();
        let config = SpillConfig::new(MIN_WORK_MEM_BYTES, dir.path().into());
        let mut tape = SpillTape::new(config.for_operator(Arc::new(QueryCancel::new())));
        for value in 0..4 {
            tape.push(Number(value)).unwrap();
        }
        tape.finish().unwrap();
        let mut first = tape.reader().unwrap();
        let mut second = tape.reader().unwrap();

        assert_eq!(first.next_record().unwrap(), Some(Number(0)));
        assert_eq!(first.next_record().unwrap(), Some(Number(1)));
        assert_eq!(second.next_record().unwrap(), Some(Number(0)));
        assert_eq!(first.next_record().unwrap(), Some(Number(2)));
        assert_eq!(second.next_record().unwrap(), Some(Number(1)));
    }

    #[test]
    fn forked_disk_reader_replays_full_sequence_independently() {
        let dir = tempfile::tempdir().unwrap();
        let config = SpillConfig::new(MIN_WORK_MEM_BYTES, dir.path().into());
        let mut tape = SpillTape::new(config.for_operator(Arc::new(QueryCancel::new())));
        for value in 0..1000 {
            tape.push(Number(value)).unwrap();
        }
        tape.finish().unwrap();
        assert!(config.stats.files_created() > 0);

        let first = tape.reader().unwrap();
        let mut readers = [first.clone(), first];
        for reader in &mut readers {
            let mut values = Vec::new();
            while let Some(value) = reader.next_record().unwrap() {
                values.push(value.0);
            }
            assert_eq!(values, (0..1000).collect::<Vec<_>>());
        }
    }

    #[test]
    fn in_memory_reader_retains_charge_after_tape_is_dropped() {
        let config = SpillConfig::new(64 * 1024, std::env::temp_dir());
        let ctx = config.for_operator(Arc::new(QueryCancel::new()));
        let mut tape = SpillTape::new(ctx.clone());
        tape.push(Number(1)).unwrap();
        tape.push(Number(2)).unwrap();
        tape.finish().unwrap();
        let reader = tape.reader().unwrap();
        let charged = ctx.reserved_bytes();
        assert!(charged > 0);

        drop(tape);
        assert_eq!(ctx.reserved_bytes(), charged);
        drop(reader);
        assert_eq!(ctx.reserved_bytes(), 0);
    }
    #[test]
    fn external_sort_is_stable_across_runs() {
        let d = tempfile::tempdir().unwrap();
        let c = SpillConfig::new(MIN_WORK_MEM_BYTES, d.path().into());
        let ctx = c.for_operator(Arc::new(QueryCancel::new()));
        let mut s = ExternalSorter::new(ctx, |a: &Number, b: &Number| (a.0 % 3).cmp(&(b.0 % 3)));
        for i in 0..3000 {
            s.push(Number(i)).unwrap();
        }
        let mut out = s.finish().unwrap();
        let mut got = Vec::new();
        while let Some(v) = out.next_record().unwrap() {
            got.push(v.0);
        }
        let mut expected = (0..3000).collect::<Vec<_>>();
        expected.sort_by_key(|v| v % 3);
        assert_eq!(got, expected);
        assert!(c.stats.files_created() > 0);
    }

    #[test]
    fn in_memory_sorted_stream_observes_late_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = Arc::new(QueryCancel::new());
        let config = SpillConfig::new(4096, dir.path().into());
        let mut sorter = ExternalSorter::new(
            config.for_operator(cancel.clone()),
            |left: &Number, right: &Number| left.0.cmp(&right.0),
        );
        sorter.push(Number(2)).unwrap();
        sorter.push(Number(1)).unwrap();
        let mut output = sorter.finish().unwrap();
        cancel.request(CancelReason::StatementTimeout);

        let err = output.next_record().unwrap_err();
        assert_eq!(err.code, common::SqlState::QueryCanceled);
    }

    #[test]
    fn external_sort_observes_cancellation_requested_during_sort() {
        use std::cell::Cell;

        let cancel = Arc::new(QueryCancel::new());
        let requested = Cell::new(false);
        let comparator_cancel = cancel.clone();
        let config = SpillConfig::new(64 * 1024, std::env::temp_dir());
        let mut sorter = ExternalSorter::new(
            config.for_operator(cancel),
            |left: &Number, right: &Number| {
                if !requested.replace(true) {
                    comparator_cancel.request(CancelReason::StatementTimeout);
                }
                left.0.cmp(&right.0)
            },
        );
        for value in (0..100).rev() {
            sorter.push(Number(value)).unwrap();
        }

        let err = sorter.finish().err().expect("sort should be canceled");
        assert_eq!(err.code, common::SqlState::QueryCanceled);
        assert!(requested.get());
    }

    #[test]
    fn exec_row_codec_preserves_special_values() {
        let row = ExecRow {
            row: Row {
                values: vec![
                    Value::Float(f64::NAN.into()),
                    Value::Float((-0.0).into()),
                    Value::Numeric(Decimal::new(150, 2)),
                    Value::Bytes(vec![0, 255]),
                ],
            },
            identity: Some(RowIdentity {
                row_id: RowId {
                    page_num: 3,
                    slot_num: 4,
                },
                xmin: 11,
                key: Key(vec![Value::Integer(9)]),
            }),
        };
        let mut bytes = Vec::new();
        codec::encode_exec_row(&row, &mut bytes).unwrap();
        let decoded = codec::decode_exec_row(&mut bytes.as_slice()).unwrap();
        assert_eq!(decoded, row);
        assert!(matches!(decoded.row.values[0],Value::Float(v) if v.0.is_nan()));
    }

    #[test]
    fn value_and_row_tapes_round_trip_every_value_variant() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Integer(i64::MIN),
            Value::Float(f64::INFINITY.into()),
            Value::Float((-0.0).into()),
            Value::Real(f32::NEG_INFINITY.into()),
            Value::Numeric(Decimal::new(-12345, 3)),
            Value::Text("héllo".into()),
            Value::Date(-12),
            Value::Timestamp(123_456),
            Value::Time(42),
            Value::TimestampTz(-987_654),
            Value::Interval(common::Interval {
                months: -2,
                days: 3,
                micros: -4,
            }),
            Value::Bytes(vec![0, 1, 255]),
            Value::Uuid([0xab; 16]),
            Value::Array(
                SqlArray::new(
                    DataType::Integer,
                    vec![ArrayDimension::new(3, -1)],
                    vec![Value::Integer(7), Value::Null, Value::Integer(9)],
                )
                .unwrap(),
            ),
        ];
        let config = SpillConfig::new(MIN_WORK_MEM_BYTES, std::env::temp_dir());
        let ctx = config.for_operator(Arc::new(QueryCancel::new()));
        let mut value_tape = SpillTape::disk_only(ctx.clone()).unwrap();
        for value in &values {
            value_tape.push(value.clone()).unwrap();
        }
        value_tape.finish().unwrap();
        let mut reader = value_tape.reader().unwrap();
        for value in &values {
            assert_eq!(reader.next_record().unwrap().as_ref(), Some(value));
        }
        assert_eq!(reader.next_record().unwrap(), None);

        let row = Row { values };
        let mut row_tape = SpillTape::disk_only(ctx).unwrap();
        row_tape.push(row.clone()).unwrap();
        row_tape.finish().unwrap();
        assert_eq!(row_tape.reader().unwrap().next_record().unwrap(), Some(row));
    }
}
