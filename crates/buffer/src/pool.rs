use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use common::{DbError, FileId, FlushPolicy, PageFlushInfo, PageNum, Result, SqlState};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, Mutex, RawRwLock, RwLock};

use crate::{PAGE_SIZE, PageData, PageInfo, PageLoader, PageStore};

type PageKey = (FileId, PageNum);
type PageReadLatch = ArcRwLockReadGuard<RawRwLock, PageData>;
type PageWriteLatch = ArcRwLockWriteGuard<RawRwLock, PageData>;

pub struct PageReadGuard {
    file_id: FileId,
    page_num: PageNum,
    frame: Arc<Frame>,
    guard: PageReadLatch,
}

impl PageReadGuard {
    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn page_num(&self) -> PageNum {
        self.page_num
    }

    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.guard.0
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        self.frame.unpin();
    }
}

impl fmt::Debug for PageReadGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageReadGuard")
            .field("file_id", &self.file_id)
            .field("page_num", &self.page_num)
            .finish_non_exhaustive()
    }
}

pub struct PageWriteGuard {
    file_id: FileId,
    page_num: PageNum,
    frame: Arc<Frame>,
    guard: PageWriteLatch,
}

impl PageWriteGuard {
    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn page_num(&self) -> PageNum {
        self.page_num
    }

    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.guard.0
    }

    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.guard.0
    }

    /// Atomically take the "needs full-page image" flag for this page, returning
    /// whether this is the first modification since the last checkpoint. The
    /// caller logs a `FullPageImage` when true, else a delta record.
    pub fn take_needs_fpi(&self) -> bool {
        self.frame.needs_fpi.swap(false, Ordering::AcqRel)
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        self.frame.unpin();
    }
}

impl fmt::Debug for PageWriteGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageWriteGuard")
            .field("file_id", &self.file_id)
            .field("page_num", &self.page_num)
            .finish_non_exhaustive()
    }
}

pub trait BufferPool: Send + Sync {
    fn read_page(&self, file_id: FileId, page_num: PageNum) -> Result<PageReadGuard>;
    fn write_page(&self, file_id: FileId, page_num: PageNum, txn_id: u64)
    -> Result<PageWriteGuard>;
    fn new_page(&self, file_id: FileId, txn_id: u64) -> Result<PageWriteGuard>;
    fn load_page(&self, file_id: FileId, page_num: PageNum, data: PageData) -> Result<()>;
    fn iter_pages(&self) -> Result<Box<dyn Iterator<Item = PageInfo>>>;
    fn mark_all_clean(&self) -> Result<()>;
    fn rollback(&self, txn_id: u64) -> Result<()>;
    fn commit(&self, txn_id: u64) -> Result<()>;

    /// Write every flushable dirty page (per the flush policy) to its home in the
    /// `PageStore`. Does not fsync or mark frames clean; the caller fsyncs via the
    /// store and then calls `mark_all_clean`. Used by checkpoint.
    fn flush_committed_pages(&self) -> Result<()>;

    /// Obtain a writable frame for recovery redo, creating a zeroed frame when the
    /// page is absent from the store (a new page being re-established). The frame
    /// is marked dirty under the recovery txn id (0) so it is flushed by the
    /// post-recovery checkpoint.
    fn fetch_for_redo(&self, file_id: FileId, page_num: PageNum) -> Result<PageWriteGuard>;

    /// Allow eviction to flush+evict committed dirty pages (steal). Disabled until
    /// the server enables it during startup (before redo).
    fn enable_stealing(&self);
}

pub struct MemoryBufferPool {
    frame_count: usize,
    flush_policy: Box<dyn FlushPolicy>,
    store: Arc<dyn PageStore>,
    state: Mutex<PoolState>,
    /// When true, eviction may flush a committed dirty page to its home and evict
    /// it (steal). Off until the server enables it during startup.
    stealing: AtomicBool,
}

impl MemoryBufferPool {
    pub fn new(
        frame_count: usize,
        flush_policy: Box<dyn FlushPolicy>,
        store: Arc<dyn PageStore>,
    ) -> Self {
        Self {
            frame_count,
            flush_policy,
            store,
            state: Mutex::new(PoolState::default()),
            stealing: AtomicBool::new(false),
        }
    }

    pub fn empty(frame_count: usize) -> Self {
        Self::new(frame_count, Box::new(NeverFlush), Arc::new(NoopPageStore))
    }

    fn read_resident_page(&self, file_id: FileId, page_num: PageNum) -> Option<PageReadGuard> {
        let frame = {
            let state = self.state.lock();
            let frame = state.frames.get(&(file_id, page_num)).cloned()?;
            frame.pin();
            frame
        };
        Some(read_guard(file_id, page_num, frame))
    }

    /// Run `attempt` under the pool lock. `Ok(Some)` succeeds; `Ok(None)` means the
    /// pool is full, so free one frame (flushing a committed dirty victim outside
    /// the lock when stealing is enabled) and retry.
    fn with_room<T>(
        &self,
        mut attempt: impl FnMut(&mut PoolState) -> Result<Option<T>>,
    ) -> Result<T> {
        loop {
            let outcome = {
                let mut state = self.state.lock();
                attempt(&mut state)?
            };
            match outcome {
                Some(value) => return Ok(value),
                None => self.make_room()?,
            }
        }
    }

    /// Free one frame. A clean unpinned frame is dropped under the lock; a
    /// committed dirty victim (stealing enabled and the flush policy admits it)
    /// is flushed to its home outside the lock, then evicted.
    fn make_room(&self) -> Result<()> {
        loop {
            let victim = {
                let mut state = self.state.lock();
                if state.frames.len() < self.frame_count {
                    return Ok(());
                }
                match state.reclaim_victim(
                    self.stealing.load(Ordering::Acquire),
                    self.flush_policy.as_ref(),
                ) {
                    ReclaimOutcome::FreedClean => return Ok(()),
                    ReclaimOutcome::ReservedDirty(frame) => frame,
                    ReclaimOutcome::NoVictim => {
                        return Err(Self::storage_internal_error(
                            "no unpinned frame available for eviction",
                        ));
                    }
                }
            };

            // Flush the reserved victim to its home without holding the pool lock.
            // Safety rests on the per-frame latch and pin discipline, not on any
            // global controller (lock-free readers take no controller guard): a
            // victim is only reclaimed when unpinned, and any frame with an
            // in-flight write is pinned by its live `PageWriteGuard`, so it is never
            // chosen here. We snapshot the page bytes under the frame's read latch
            // (`victim.data.read()`), so a concurrent reader or a writer on a
            // *different* frame cannot tear this copy. The reservation pin keeps
            // another eviction from racing this one; a reader that pins the victim
            // during the flush is detected below.
            let data = victim.data.read().clone();
            self.store
                .write_page(victim.file_id, victim.page_num, &data)?;

            let mut state = self.state.lock();
            victim.unpin(); // release the flush reservation
            if victim.pin_count.load(Ordering::Acquire) == 0 {
                victim.mark_clean();
                state.remove_frame((victim.file_id, victim.page_num));
                return Ok(());
            }
            // A reader pinned the victim during the flush; try another frame.
        }
    }

    /// Seed the page allocator for `file_id` from its on-disk extent the first
    /// time the file is allocated into, so a freshly allocated page never reuses
    /// one that already exists on disk. Recovery no longer preloads pages, so
    /// without this the counter would start at 0 for checkpointed-but-not-replayed
    /// files and `new_page` would overwrite committed data.
    ///
    /// The extent read happens outside the pool lock; that is sound because the
    /// single writer holds the server's exclusive statement guard, so no
    /// concurrent flush can extend the file between the read and the seed.
    fn ensure_extent_seeded(&self, file_id: FileId) -> Result<()> {
        if self.state.lock().extent_seeded.contains(&file_id) {
            return Ok(());
        }
        let extent = self.store.page_count(file_id)?;
        let mut state = self.state.lock();
        if state.extent_seeded.insert(file_id) {
            state.ensure_next_page_at_least(file_id, extent);
        }
        Ok(())
    }

    fn insert_loaded_read_page(
        &self,
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
    ) -> Result<PageReadGuard> {
        let frame = self.with_room(|state| {
            let Some(frame) = state.get_or_insert_clean(self.frame_count, file_id, page_num, &data)
            else {
                return Ok(None);
            };
            state.advance_next_page_num(file_id, page_num);
            frame.pin();
            Ok(Some(frame))
        })?;
        Ok(read_guard(file_id, page_num, frame))
    }

    fn insert_loaded_write_page(
        &self,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
        data: PageData,
    ) -> Result<Arc<Frame>> {
        self.with_room(|state| {
            let Some(frame) = state.get_or_insert_clean(self.frame_count, file_id, page_num, &data)
            else {
                return Ok(None);
            };
            state.advance_next_page_num(file_id, page_num);
            state.record_before_image(txn_id, file_id, page_num, &frame);
            frame.mark_dirty(txn_id);
            frame.pin();
            Ok(Some(frame))
        })
    }

    fn insert_clean_page_if_absent(
        &self,
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
    ) -> Result<()> {
        self.with_room(|state| {
            if state
                .get_or_insert_clean(self.frame_count, file_id, page_num, &data)
                .is_none()
            {
                return Ok(None);
            }
            state.advance_next_page_num(file_id, page_num);
            Ok(Some(()))
        })
    }

    fn prepare_write_frame(
        &self,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
    ) -> Option<Arc<Frame>> {
        let mut state = self.state.lock();
        let frame = state.frames.get(&(file_id, page_num)).cloned()?;
        state.record_before_image(txn_id, file_id, page_num, &frame);
        frame.mark_dirty(txn_id);
        frame.pin();
        Some(frame)
    }

    fn storage_internal_error(message: impl Into<String>) -> DbError {
        DbError::storage(SqlState::InternalError, message)
    }
}

impl BufferPool for MemoryBufferPool {
    fn read_page(&self, file_id: FileId, page_num: PageNum) -> Result<PageReadGuard> {
        if let Some(guard) = self.read_resident_page(file_id, page_num) {
            return Ok(guard);
        }

        match self.store.load_page(file_id, page_num)? {
            Some(data) => self.insert_loaded_read_page(file_id, page_num, data),
            None => Err(Self::storage_internal_error(format!(
                "page not found: file_id={file_id}, page_num={page_num}"
            ))),
        }
    }

    fn write_page(
        &self,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
    ) -> Result<PageWriteGuard> {
        let frame = if let Some(frame) = self.prepare_write_frame(file_id, page_num, txn_id) {
            frame
        } else {
            match self.store.load_page(file_id, page_num)? {
                Some(data) => self.insert_loaded_write_page(file_id, page_num, txn_id, data)?,
                None => {
                    return Err(Self::storage_internal_error(format!(
                        "page not found: file_id={file_id}, page_num={page_num}"
                    )));
                }
            }
        };
        Ok(write_guard(file_id, page_num, frame))
    }

    fn new_page(&self, file_id: FileId, txn_id: u64) -> Result<PageWriteGuard> {
        self.ensure_extent_seeded(file_id)?;
        let (page_num, frame) = self.with_room(|state| {
            if state.frames.len() >= self.frame_count {
                return Ok(None);
            }
            let page_num = state.next_page_num(file_id);
            let frame = state.insert_fresh_frame(file_id, page_num)?;
            frame.mark_dirty(txn_id);
            state.advance_next_page_num(file_id, page_num);
            let txn = state.txns.entry(txn_id).or_default();
            txn.new_pages.push((file_id, page_num));
            // `page_num` of the txn's first allocation in this file is its
            // pre-txn allocation counter; remember it once for rollback.
            txn.next_page_before.entry(file_id).or_insert(page_num);
            frame.pin();
            Ok(Some((page_num, frame)))
        })?;
        Ok(write_guard(file_id, page_num, frame))
    }

    fn load_page(&self, file_id: FileId, page_num: PageNum, data: PageData) -> Result<()> {
        self.insert_clean_page_if_absent(file_id, page_num, data)
    }

    fn iter_pages(&self) -> Result<Box<dyn Iterator<Item = PageInfo>>> {
        let frames: Vec<_> = {
            let state = self.state.lock();
            let mut keys: Vec<_> = state.frames.keys().copied().collect();
            keys.sort_unstable();
            keys.into_iter()
                .filter_map(|key| state.frames.get(&key).cloned())
                .collect()
        };

        let pages: Vec<_> = frames
            .into_iter()
            .map(|frame| PageInfo {
                file_id: frame.file_id,
                page_num: frame.page_num,
                data: frame.data.read().clone(),
                is_dirty: frame.is_dirty(),
            })
            .collect();
        Ok(Box::new(pages.into_iter()))
    }

    fn mark_all_clean(&self) -> Result<()> {
        let mut state = self.state.lock();
        for frame in state.frames.values() {
            frame.mark_clean();
        }
        state.txns.clear();
        Ok(())
    }

    fn rollback(&self, txn_id: u64) -> Result<()> {
        let dirty_state = {
            let mut state = self.state.lock();
            state.txns.remove(&txn_id)
        };

        let Some(dirty_state) = dirty_state else {
            return Ok(());
        };

        let new_pages: HashSet<_> = dirty_state.new_pages.into_iter().collect();

        {
            let mut state = self.state.lock();
            for key in &new_pages {
                state.remove_frame(*key);
            }
            // Restore each file's allocation counter so the rolled-back pages are
            // re-allocatable (single writer in v1, so no other txn raised it).
            for (file_id, before) in dirty_state.next_page_before {
                state.next_page_num_by_file.insert(file_id, before);
            }
        }

        for ((file_id, page_num), before_image) in dirty_state.before_images {
            if new_pages.contains(&(file_id, page_num)) {
                continue;
            }

            if let Some(frame) = {
                let state = self.state.lock();
                state.frames.get(&(file_id, page_num)).cloned()
            } {
                *frame.data.write() = before_image.data;
                frame.restore_dirty_state(before_image.was_dirty, before_image.dirty_txn_id);
            }
        }

        Ok(())
    }

    fn commit(&self, txn_id: u64) -> Result<()> {
        self.state.lock().txns.remove(&txn_id);
        Ok(())
    }

    fn enable_stealing(&self) {
        self.stealing.store(true, Ordering::Release);
    }

    fn flush_committed_pages(&self) -> Result<()> {
        // Collect dirty frames under the lock, then do I/O without holding it.
        let dirty: Vec<Arc<Frame>> = {
            let state = self.state.lock();
            state
                .frames
                .values()
                .filter(|frame| frame.is_dirty())
                .cloned()
                .collect()
        };
        for frame in dirty {
            let info = PageFlushInfo {
                dirty_txn_id: frame.dirty_txn_id.load(Ordering::Acquire),
                page_lsn: None,
            };
            // Checkpoint runs under the exclusive write guard, so every dirty
            // page belongs to a committed (or recovery, txn 0) transaction and is
            // flushable. An unflushable dirty page would be silently dropped by
            // the subsequent `mark_all_clean`, so fail loudly instead.
            if !self.flush_policy.can_flush(&info) {
                return Err(Self::storage_internal_error(
                    "checkpoint encountered an unflushable dirty page",
                ));
            }
            let data = frame.data.read().clone();
            self.store
                .write_page(frame.file_id, frame.page_num, &data)?;
        }
        Ok(())
    }

    fn fetch_for_redo(&self, file_id: FileId, page_num: PageNum) -> Result<PageWriteGuard> {
        const RECOVERY_TXN: u64 = 0;
        if let Some(frame) = self.prepare_write_frame(file_id, page_num, RECOVERY_TXN) {
            return Ok(write_guard(file_id, page_num, frame));
        }
        let data = self.store.load_page(file_id, page_num)?.unwrap_or_default();
        let frame = self.insert_loaded_write_page(file_id, page_num, RECOVERY_TXN, data)?;
        Ok(write_guard(file_id, page_num, frame))
    }
}

#[derive(Default)]
struct PoolState {
    frames: HashMap<PageKey, Arc<Frame>>,
    clock_order: Vec<PageKey>,
    clock_hand: usize,
    next_page_num_by_file: HashMap<FileId, PageNum>,
    /// Files whose allocation counter has been seeded from the on-disk extent.
    extent_seeded: HashSet<FileId>,
    txns: HashMap<u64, TxnDirtyState>,
}

impl PoolState {
    fn next_page_num(&self, file_id: FileId) -> PageNum {
        self.next_page_num_by_file
            .get(&file_id)
            .copied()
            .unwrap_or(0)
    }

    fn advance_next_page_num(&mut self, file_id: FileId, page_num: PageNum) {
        let next = page_num.saturating_add(1);
        self.next_page_num_by_file
            .entry(file_id)
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
    }

    fn ensure_next_page_at_least(&mut self, file_id: FileId, next: PageNum) {
        self.next_page_num_by_file
            .entry(file_id)
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
    }

    /// Return the resident frame for `(file_id, page_num)`, or insert `data` as a
    /// clean frame if there is room. `None` means the pool is full; the caller
    /// frees a frame and retries. A resident page is returned unchanged (bytes,
    /// dirty state, and rollback metadata are left intact).
    fn get_or_insert_clean(
        &mut self,
        frame_count: usize,
        file_id: FileId,
        page_num: PageNum,
        data: &PageData,
    ) -> Option<Arc<Frame>> {
        let key = (file_id, page_num);
        if let Some(frame) = self.frames.get(&key) {
            frame.reference_bit.store(true, Ordering::Release);
            return Some(frame.clone());
        }
        if self.frames.len() >= frame_count {
            return None;
        }
        let frame = Arc::new(Frame::new(file_id, page_num, data.clone(), false, true));
        self.frames.insert(key, frame.clone());
        self.clock_order.push(key);
        Some(frame)
    }

    /// Insert a freshly allocated dirty page, rejecting an already-resident key.
    /// The caller guarantees there is room before allocating the page number.
    fn insert_fresh_frame(&mut self, file_id: FileId, page_num: PageNum) -> Result<Arc<Frame>> {
        let key = (file_id, page_num);
        if self.frames.contains_key(&key) {
            return Err(DbError::internal(format!(
                "page already resident: file_id={file_id}, page_num={page_num}"
            )));
        }
        let frame = Arc::new(Frame::new(
            file_id,
            page_num,
            PageData::default(),
            true,
            false,
        ));
        self.frames.insert(key, frame.clone());
        self.clock_order.push(key);
        Ok(frame)
    }

    fn remove_frame(&mut self, key: PageKey) {
        self.frames.remove(&key);
        self.clock_order.retain(|candidate| *candidate != key);
        self.fix_clock_hand();
    }

    fn advance_clock_hand(&mut self) {
        if !self.clock_order.is_empty() {
            self.clock_hand = (self.clock_hand + 1) % self.clock_order.len();
        }
    }

    fn fix_clock_hand(&mut self) {
        if self.clock_order.is_empty() {
            self.clock_hand = 0;
        } else {
            self.clock_hand %= self.clock_order.len();
        }
    }

    fn record_before_image(
        &mut self,
        txn_id: u64,
        file_id: FileId,
        page_num: PageNum,
        frame: &Frame,
    ) {
        self.txns
            .entry(txn_id)
            .or_default()
            .before_images
            .entry((file_id, page_num))
            .or_insert_with(|| BeforeImage {
                data: frame.data.read().clone(),
                was_dirty: frame.is_dirty(),
                dirty_txn_id: frame.dirty_txn_id.load(Ordering::Acquire),
            });
    }

    /// Clock-sweep for an eviction victim. A clean unpinned frame is removed
    /// immediately (`FreedClean`). When stealing is enabled, a committed dirty
    /// unpinned frame is pinned and returned (`ReservedDirty`) so the caller can
    /// flush it outside the lock before evicting. `NoVictim` means every frame is
    /// pinned or holds dirty data the flush policy refuses.
    fn reclaim_victim(&mut self, stealing: bool, flush_policy: &dyn FlushPolicy) -> ReclaimOutcome {
        let sweep_limit = self.clock_order.len().saturating_mul(2);
        for _ in 0..sweep_limit {
            if self.clock_order.is_empty() {
                break;
            }
            self.clock_hand %= self.clock_order.len();
            let key = self.clock_order[self.clock_hand];
            let Some(frame) = self.frames.get(&key).cloned() else {
                self.clock_order.remove(self.clock_hand);
                self.fix_clock_hand();
                continue;
            };

            if frame.pin_count.load(Ordering::Acquire) != 0 {
                self.advance_clock_hand();
                continue;
            }
            if frame.reference_bit.swap(false, Ordering::AcqRel) {
                self.advance_clock_hand();
                continue;
            }

            if !frame.is_dirty() {
                self.remove_frame(key);
                return ReclaimOutcome::FreedClean;
            }

            if stealing {
                let info = PageFlushInfo {
                    dirty_txn_id: frame.dirty_txn_id.load(Ordering::Acquire),
                    page_lsn: None,
                };
                if flush_policy.can_flush(&info) {
                    frame.pin(); // reserve across the unlocked flush
                    return ReclaimOutcome::ReservedDirty(frame);
                }
            }
            self.advance_clock_hand();
        }
        ReclaimOutcome::NoVictim
    }
}

/// Outcome of a clock-sweep victim search (see `PoolState::reclaim_victim`).
enum ReclaimOutcome {
    /// A clean frame was removed under the lock; room is available.
    FreedClean,
    /// A committed dirty frame was pinned for an out-of-lock flush, then eviction.
    ReservedDirty(Arc<Frame>),
    /// No frame can be evicted (all pinned or unflushable dirty).
    NoVictim,
}

#[derive(Default)]
struct TxnDirtyState {
    before_images: HashMap<PageKey, BeforeImage>,
    new_pages: Vec<PageKey>,
    /// Each file's allocation counter just before this txn first allocated into
    /// it, so rollback can restore it and fully undo the allocation. Without this
    /// a rolled-back B-tree build would leave the counter advanced, and a rebuild
    /// in the same (reused) file would not place its metapage at page 0.
    next_page_before: HashMap<FileId, PageNum>,
}

struct BeforeImage {
    data: PageData,
    was_dirty: bool,
    dirty_txn_id: u64,
}

struct Frame {
    file_id: FileId,
    page_num: PageNum,
    data: Arc<RwLock<PageData>>,
    pin_count: AtomicUsize,
    dirty: AtomicBool,
    dirty_txn_id: AtomicU64,
    reference_bit: AtomicBool,
    needs_fpi: AtomicBool,
}

impl Frame {
    fn new(
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
        dirty: bool,
        needs_fpi: bool,
    ) -> Self {
        Self {
            file_id,
            page_num,
            data: Arc::new(RwLock::new(data)),
            pin_count: AtomicUsize::new(0),
            dirty: AtomicBool::new(dirty),
            dirty_txn_id: AtomicU64::new(0),
            reference_bit: AtomicBool::new(true),
            needs_fpi: AtomicBool::new(needs_fpi),
        }
    }

    fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
        self.reference_bit.store(true, Ordering::Release);
    }

    fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn mark_dirty(&self, txn_id: u64) {
        self.dirty.store(true, Ordering::Release);
        self.dirty_txn_id.store(txn_id, Ordering::Release);
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
        self.dirty_txn_id.store(0, Ordering::Release);
        // A clean page is on disk; its next modification must log a full-page
        // image so a torn write can be repaired during redo.
        self.needs_fpi.store(true, Ordering::Release);
    }

    fn restore_dirty_state(&self, was_dirty: bool, dirty_txn_id: u64) {
        self.dirty.store(was_dirty, Ordering::Release);
        self.dirty_txn_id.store(dirty_txn_id, Ordering::Release);
        // Conservatively require a full-page image on the next modification after
        // a rollback, since the rolled-back statement's FPI (if any) is discarded.
        self.needs_fpi.store(true, Ordering::Release);
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

fn read_guard(file_id: FileId, page_num: PageNum, frame: Arc<Frame>) -> PageReadGuard {
    let guard = frame.data.read_arc();
    PageReadGuard {
        file_id,
        page_num,
        frame,
        guard,
    }
}

fn write_guard(file_id: FileId, page_num: PageNum, frame: Arc<Frame>) -> PageWriteGuard {
    let guard = frame.data.write_arc();
    PageWriteGuard {
        file_id,
        page_num,
        frame,
        guard,
    }
}

struct NeverFlush;

impl FlushPolicy for NeverFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        false
    }
}

struct NoopPageStore;

impl PageLoader for NoopPageStore {
    fn load_page(&self, _file_id: FileId, _page_num: PageNum) -> Result<Option<PageData>> {
        Ok(None)
    }
}

impl PageStore for NoopPageStore {
    fn write_page(&self, _file_id: FileId, _page_num: PageNum, _data: &PageData) -> Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> Result<()> {
        Ok(())
    }

    fn page_count(&self, _file_id: FileId) -> Result<PageNum> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use common::{DbError, ErrorKind, FileId, PageNum, Result, SqlState};

    use super::*;

    #[test]
    fn rollback_restores_original_before_image_even_after_multiple_writes() {
        let pool = MemoryBufferPool::empty(8);
        let txn = 11;

        {
            let mut page = pool.new_page(1, txn).unwrap();
            page.data_mut()[0] = 10;
        }
        pool.commit(txn).unwrap();

        {
            let mut page = pool.write_page(1, 0, 12).unwrap();
            page.data_mut()[0] = 20;
        }
        {
            let mut page = pool.write_page(1, 0, 12).unwrap();
            page.data_mut()[0] = 30;
        }

        pool.rollback(12).unwrap();

        let page = pool.read_page(1, 0).unwrap();
        assert_eq!(page.data()[0], 10);
        drop(page);

        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_dirty);
    }

    #[test]
    fn rollback_removes_new_pages_from_failed_transaction() {
        let pool = MemoryBufferPool::empty(8);

        {
            let mut page = pool.new_page(1, 77).unwrap();
            page.data_mut()[0] = 99;
        }

        pool.rollback(77).unwrap();

        assert!(pool.read_page(1, 0).is_err());
    }

    #[test]
    fn rollback_resets_allocation_so_new_pages_reuse_numbers() {
        let pool = MemoryBufferPool::empty(8);

        // A transaction allocates two pages in a fresh file, then rolls back.
        {
            let _meta = pool.new_page(1, 7).unwrap();
            let _root = pool.new_page(1, 7).unwrap();
        }
        pool.rollback(7).unwrap();

        // The next transaction's first allocation reuses page 0, so a rebuilt
        // B-tree in the same file places its metapage at page 0 again.
        let page = pool.new_page(1, 8).unwrap();
        assert_eq!(page.page_num(), 0);
    }

    #[test]
    fn commit_leaves_page_dirty_until_mark_all_clean() {
        let pool = MemoryBufferPool::empty(8);

        {
            let mut page = pool.new_page(1, 1).unwrap();
            page.data_mut()[0] = 1;
        }
        pool.commit(1).unwrap();

        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_dirty);

        pool.mark_all_clean().unwrap();

        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert!(!pages[0].is_dirty);
    }

    #[test]
    fn mark_all_clean_makes_previously_dirty_pages_evictable() {
        let pool = MemoryBufferPool::empty(1);
        {
            let mut page = pool.new_page(1, 1).unwrap();
            page.data_mut()[0] = 1;
        }
        pool.commit(1).unwrap();
        pool.mark_all_clean().unwrap();

        pool.load_page(1, 1, data_with_first_byte(2)).unwrap();

        assert!(pool.read_page(1, 0).is_err());
        assert_eq!(pool.read_page(1, 1).unwrap().data()[0], 2);
    }

    #[test]
    fn rollback_of_clean_page_leaves_it_evictable() {
        let pool = MemoryBufferPool::empty(1);
        pool.load_page(1, 0, data_with_first_byte(1)).unwrap();
        {
            let mut page = pool.write_page(1, 0, 1).unwrap();
            page.data_mut()[0] = 9;
        }

        pool.rollback(1).unwrap();
        pool.load_page(1, 1, data_with_first_byte(2)).unwrap();

        assert!(pool.read_page(1, 0).is_err());
        assert_eq!(pool.read_page(1, 1).unwrap().data()[0], 2);
    }

    #[test]
    fn load_page_advances_next_page_number() {
        let pool = MemoryBufferPool::empty(8);
        pool.load_page(7, 3, PageData::default()).unwrap();

        let page = pool.new_page(7, 1).unwrap();

        assert_eq!(page.page_num(), 4);
    }

    #[test]
    fn load_page_does_not_overwrite_resident_dirty_page() {
        let pool = MemoryBufferPool::empty(8);
        pool.load_page(1, 0, data_with_first_byte(1)).unwrap();

        {
            let mut page = pool.write_page(1, 0, 77).unwrap();
            page.data_mut()[0] = 9;
        }

        pool.load_page(1, 0, data_with_first_byte(2)).unwrap();

        assert_eq!(pool.read_page(1, 0).unwrap().data()[0], 9);
        pool.rollback(77).unwrap();
        assert_eq!(pool.read_page(1, 0).unwrap().data()[0], 1);
    }

    #[test]
    fn insert_fresh_frame_rejects_resident_page_key() {
        let mut state = PoolState::default();
        state.insert_fresh_frame(1, 0).unwrap();

        let err = match state.insert_fresh_frame(1, 0) {
            Ok(_) => panic!("expected resident page rejection"),
            Err(err) => err,
        };

        assert_eq!(err.kind, ErrorKind::Internal);
        assert!(err.message.contains("already resident"));
        assert_eq!(state.frames.get(&(1, 0)).unwrap().data.read().0[0], 0);
    }

    #[test]
    fn read_page_loader_result_is_pinned_before_returning() {
        let loader = Arc::new(TestPageLoader::new([((2, 5), data_with_first_byte(5))]));
        let pool = MemoryBufferPool::new(1, Box::new(NeverFlush), loader);

        let guard = pool.read_page(2, 5).unwrap();
        let err = pool.load_page(2, 6, data_with_first_byte(6)).unwrap_err();

        assert_eq!(guard.data()[0], 5);
        assert_eq!(err.kind, ErrorKind::Storage);
        assert_eq!(err.code, SqlState::InternalError);
    }

    #[test]
    fn dirty_pages_are_skipped_by_eviction() {
        let pool = MemoryBufferPool::empty(1);
        {
            let mut page = pool.new_page(1, 1).unwrap();
            page.data_mut()[0] = 42;
        }

        let err = pool.load_page(1, 1, PageData::default()).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Storage);
        assert_eq!(err.code, SqlState::InternalError);
    }

    #[test]
    fn iter_pages_returns_dirty_in_memory_data() {
        let pool = MemoryBufferPool::empty(8);
        {
            let mut page = pool.new_page(1, 1).unwrap();
            page.data_mut()[0] = 55;
        }

        let pages: Vec<_> = pool.iter_pages().unwrap().collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].data.0[0], 55);
        assert!(pages[0].is_dirty);
    }

    #[test]
    fn read_page_loads_from_page_loader_on_miss() {
        let loader = Arc::new(TestPageLoader::new([((2, 5), data_with_first_byte(88))]));
        let pool = MemoryBufferPool::new(8, Box::new(NeverFlush), loader.clone());

        let page = pool.read_page(2, 5).unwrap();

        assert_eq!(page.data()[0], 88);
        assert_eq!(loader.calls(), vec![(2, 5)]);
    }

    #[test]
    fn write_page_loads_from_page_loader_on_miss() {
        let loader = Arc::new(TestPageLoader::new([((2, 5), data_with_first_byte(88))]));
        let pool = MemoryBufferPool::new(8, Box::new(NeverFlush), loader.clone());

        {
            let mut page = pool.write_page(2, 5, 99).unwrap();
            assert_eq!(page.data()[0], 88);
            page.data_mut()[0] = 99;
        }

        assert_eq!(loader.calls(), vec![(2, 5)]);
        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_dirty);
        assert_eq!(pages[0].data.0[0], 99);

        pool.rollback(99).unwrap();
        let page = pool.read_page(2, 5).unwrap();
        assert_eq!(page.data()[0], 88);
    }

    #[test]
    fn read_page_returns_page_not_found_when_loader_misses() {
        let pool =
            MemoryBufferPool::new(8, Box::new(NeverFlush), Arc::new(TestPageLoader::empty()));

        let err = pool.read_page(2, 5).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Storage);
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("page not found"));
    }

    #[test]
    fn pinned_clean_pages_are_not_evicted() {
        let pool = MemoryBufferPool::empty(1);
        pool.load_page(1, 0, data_with_first_byte(1)).unwrap();
        let pinned = pool.read_page(1, 0).unwrap();

        let err = pool.load_page(1, 1, data_with_first_byte(2)).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Storage);
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(pinned.data()[0], 1);
        drop(pinned);

        pool.load_page(1, 1, data_with_first_byte(2)).unwrap();
        assert_eq!(pool.read_page(1, 1).unwrap().data()[0], 2);
    }

    fn data_with_first_byte(value: u8) -> PageData {
        let mut data = PageData::default();
        data.0[0] = value;
        data
    }

    struct NeverFlush;

    impl FlushPolicy for NeverFlush {
        fn can_flush(&self, _info: &common::PageFlushInfo) -> bool {
            false
        }
    }

    struct TestPageLoader {
        pages: HashMap<(FileId, PageNum), PageData>,
        calls: Mutex<Vec<(FileId, PageNum)>>,
        error: Option<DbError>,
    }

    impl TestPageLoader {
        fn new<const N: usize>(pages: [((FileId, PageNum), PageData); N]) -> Self {
            Self {
                pages: HashMap::from(pages),
                calls: Mutex::new(Vec::new()),
                error: None,
            }
        }

        fn empty() -> Self {
            Self::new([])
        }

        fn calls(&self) -> Vec<(FileId, PageNum)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl PageLoader for TestPageLoader {
        fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
            self.calls.lock().unwrap().push((file_id, page_num));
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            Ok(self.pages.get(&(file_id, page_num)).cloned())
        }
    }

    impl PageStore for TestPageLoader {
        fn write_page(&self, _file_id: FileId, _page_num: PageNum, _data: &PageData) -> Result<()> {
            Ok(())
        }

        fn sync_all(&self) -> Result<()> {
            Ok(())
        }

        fn page_count(&self, file_id: FileId) -> Result<PageNum> {
            Ok(self
                .pages
                .keys()
                .filter(|(file, _)| *file == file_id)
                .map(|(_, page)| page + 1)
                .max()
                .unwrap_or(0))
        }
    }

    struct FlushAll;

    impl FlushPolicy for FlushAll {
        fn can_flush(&self, _info: &common::PageFlushInfo) -> bool {
            true
        }
    }

    #[derive(Default)]
    struct CapturingStore {
        writes: Mutex<Vec<(FileId, PageNum, PageData)>>,
    }

    impl PageLoader for CapturingStore {
        fn load_page(&self, _file_id: FileId, _page_num: PageNum) -> Result<Option<PageData>> {
            Ok(None)
        }
    }

    impl PageStore for CapturingStore {
        fn write_page(&self, file_id: FileId, page_num: PageNum, data: &PageData) -> Result<()> {
            self.writes
                .lock()
                .unwrap()
                .push((file_id, page_num, data.clone()));
            Ok(())
        }

        fn sync_all(&self) -> Result<()> {
            Ok(())
        }

        fn page_count(&self, _file_id: FileId) -> Result<PageNum> {
            Ok(0)
        }
    }

    #[test]
    fn flush_committed_pages_writes_dirty_pages_to_store() {
        let store = Arc::new(CapturingStore::default());
        let pool = MemoryBufferPool::new(8, Box::new(FlushAll), store.clone());
        {
            let mut page = pool.new_page(1, 5).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(5).unwrap();

        pool.flush_committed_pages().unwrap();

        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 1);
        assert_eq!(writes[0].1, 0);
        assert_eq!(writes[0].2.0[0], 42);
    }

    #[test]
    fn flush_committed_pages_errors_on_unflushable_dirty_page() {
        let store = Arc::new(CapturingStore::default());
        let pool = MemoryBufferPool::new(8, Box::new(NeverFlush), store.clone());
        {
            let mut page = pool.new_page(1, 5).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(5).unwrap();

        // A dirty page that the policy refuses must fail loudly, never be silently
        // dropped (it would be lost by the subsequent mark_all_clean).
        let err = pool.flush_committed_pages().unwrap_err();
        assert_eq!(err.kind, ErrorKind::Storage);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    /// A `PageStore` that both records writes and serves them back, so eviction
    /// spills can be read back.
    #[derive(Default)]
    struct MemStore {
        pages: Mutex<HashMap<(FileId, PageNum), PageData>>,
    }

    impl PageLoader for MemStore {
        fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
            Ok(self
                .pages
                .lock()
                .unwrap()
                .get(&(file_id, page_num))
                .cloned())
        }
    }

    impl PageStore for MemStore {
        fn write_page(&self, file_id: FileId, page_num: PageNum, data: &PageData) -> Result<()> {
            self.pages
                .lock()
                .unwrap()
                .insert((file_id, page_num), data.clone());
            Ok(())
        }

        fn sync_all(&self) -> Result<()> {
            Ok(())
        }

        fn page_count(&self, file_id: FileId) -> Result<PageNum> {
            Ok(self
                .pages
                .lock()
                .unwrap()
                .keys()
                .filter(|(file, _)| *file == file_id)
                .map(|(_, page)| page + 1)
                .max()
                .unwrap_or(0))
        }
    }

    #[test]
    fn stealing_flushes_committed_dirty_page_on_eviction() {
        let store = Arc::new(MemStore::default());
        let pool = MemoryBufferPool::new(1, Box::new(FlushAll), store.clone());
        pool.enable_stealing();

        {
            let mut page = pool.new_page(1, 7).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(7).unwrap();

        // Allocating a second page in a one-frame pool must steal page (1, 0).
        {
            let mut page = pool.new_page(1, 8).unwrap();
            page.data_mut()[0] = 99;
        }
        pool.commit(8).unwrap();

        assert!(store.pages.lock().unwrap().contains_key(&(1, 0)));
        let restored = pool.read_page(1, 0).unwrap();
        assert_eq!(restored.data()[0], 42);
    }

    #[test]
    fn stealing_cannot_evict_dirty_page_the_policy_refuses() {
        let store = Arc::new(MemStore::default());
        let pool = MemoryBufferPool::new(1, Box::new(NeverFlush), store.clone());
        pool.enable_stealing();

        {
            let mut page = pool.new_page(1, 7).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(7).unwrap();

        // The dirty page is uncommitted from the policy's view, so it cannot be
        // stolen; eviction fails loudly instead of dropping it.
        let err = pool.new_page(1, 8).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Storage);
        assert!(store.pages.lock().unwrap().is_empty());
    }

    #[test]
    fn dirty_page_is_not_stolen_when_stealing_disabled() {
        let store = Arc::new(MemStore::default());
        // FlushAll would admit the page, but stealing is never enabled.
        let pool = MemoryBufferPool::new(1, Box::new(FlushAll), store.clone());

        {
            let mut page = pool.new_page(1, 7).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(7).unwrap();

        let err = pool.new_page(1, 8).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Storage);
        assert!(store.pages.lock().unwrap().is_empty());
    }

    #[test]
    fn stealing_spills_working_set_larger_than_pool() {
        let store = Arc::new(MemStore::default());
        let pool = MemoryBufferPool::new(2, Box::new(FlushAll), store.clone());
        pool.enable_stealing();

        // Six committed pages through a two-frame pool: the rest must spill.
        for i in 0..6u8 {
            let txn = u64::from(i) + 1;
            {
                let mut page = pool.new_page(1, txn).unwrap();
                page.data_mut()[0] = i;
            }
            pool.commit(txn).unwrap();
        }

        for i in 0..6u8 {
            let page = pool.read_page(1, u32::from(i)).unwrap();
            assert_eq!(page.data()[0], i);
        }
    }

    #[test]
    fn fetch_for_redo_creates_zeroed_frame_for_missing_page() {
        let pool = MemoryBufferPool::empty(8);

        {
            let mut page = pool.fetch_for_redo(3, 0).unwrap();
            assert_eq!(page.data()[0], 0);
            page.data_mut()[0] = 7;
        }

        assert_eq!(pool.read_page(3, 0).unwrap().data()[0], 7);
    }

    #[test]
    fn take_needs_fpi_is_true_first_then_false() {
        let pool = MemoryBufferPool::empty(8);
        pool.load_page(1, 0, PageData::default()).unwrap();

        let page = pool.write_page(1, 0, 9).unwrap();
        // A loaded (on-disk) page needs a full-page image on first modification.
        assert!(page.take_needs_fpi());
        assert!(!page.take_needs_fpi());
    }
}
