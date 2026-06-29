use std::collections::{BTreeSet, HashMap, HashSet};
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
    unpublished_new: bool,
    bytes_published: bool,
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
        self.bytes_published = true;
        &mut self.guard.0
    }

    /// Atomically take the "needs full-page image" flag for this page, returning
    /// whether this is the first modification since the last checkpoint. The
    /// caller logs a `FullPageImage` when true, else a delta record.
    pub fn take_needs_fpi(&self) -> bool {
        self.frame.needs_fpi.swap(false, Ordering::AcqRel)
    }

    /// Restore the "needs full-page image" flag after a failed first-touch WAL
    /// attempt. Callers hold this page's write latch, so no other writer can have
    /// completed the first post-checkpoint modification in between.
    pub fn restore_needs_fpi(&self) {
        self.frame.needs_fpi.store(true, Ordering::Release);
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

    /// The number of pages in `file_id`'s full extent: `max(on-disk extent, the
    /// highest page allocated in memory)`. Unlike [`BufferPool::iter_pages`] (which
    /// reports only *resident* frames), this counts every page `0..page_count` that
    /// has ever existed for the file, including pages currently evicted to disk and
    /// freshly allocated pages not yet flushed. A full-extent scan (VACUUM,
    /// `docs/specs/mvcc.md` §9) iterates `0..page_count` and faults each page in via
    /// [`BufferPool::read_page`]/[`BufferPool::write_page`], so an evicted dead tuple
    /// is never missed.
    fn page_count(&self, file_id: FileId) -> Result<PageNum>;
    fn abandon_unpublished_new_page(&self, guard: PageWriteGuard) -> Result<()>;
    fn is_page_abandoned(&self, file_id: FileId, page_num: PageNum) -> bool;
    fn mark_all_clean(&self) -> Result<()>;
    /// Abort cleanup is status-based (`docs/specs/mvcc.md` §4 Decision 3): no
    /// page bytes are undone and freshly allocated pages are not reclaimed. Clears
    /// only per-transaction bookkeeping.
    fn rollback(&self, txn_id: u64) -> Result<()>;
    fn commit(&self, txn_id: u64) -> Result<()>;

    /// Write every flushable dirty page (per the flush policy) to its home in the
    /// `PageStore`, regardless of whether its dirtying transaction committed. Does
    /// not fsync or mark frames clean; the caller fsyncs via the store and then
    /// calls `mark_all_clean`. Used by checkpoint (`docs/specs/mvcc.md` §8).
    fn flush_dirty_pages(&self) -> Result<()>;

    /// Obtain a writable frame for recovery redo, creating a zeroed frame when the
    /// page is absent from the store (a new page being re-established). The frame
    /// is marked dirty under the recovery txn id (0) so it is flushed by the
    /// post-recovery checkpoint.
    fn fetch_for_redo(&self, file_id: FileId, page_num: PageNum) -> Result<PageWriteGuard>;

    /// Allow eviction to flush+evict WAL-durable dirty pages (steal). Disabled until
    /// the server enables it during startup (before redo).
    fn enable_stealing(&self);
}

pub struct MemoryBufferPool {
    frame_count: usize,
    flush_policy: Box<dyn FlushPolicy>,
    store: Arc<dyn PageStore>,
    state: Mutex<PoolState>,
    /// When true, eviction may flush a WAL-durable dirty page to its home and evict
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

    /// Look up a resident frame for use, classifying the page-table state under the
    /// pool lock so an in-transition (`evicting`) frame is never handed out
    /// (Milestone E2b). `Found` pins the frame ready to use; `Evicting` means a steal
    /// is flushing this exact page — the caller must drop the lock, yield, and retry
    /// (after the steal removes it, a fresh load from disk sees the flushed bytes);
    /// `Absent` means load it from the store.
    fn lookup_resident(&self, file_id: FileId, page_num: PageNum) -> ResidentLookup {
        let state = self.state.lock();
        match state.frames.get(&(file_id, page_num)) {
            Some(frame) if frame.evicting.load(Ordering::Acquire) => ResidentLookup::Evicting,
            Some(frame) => {
                let frame = frame.clone();
                frame.pin();
                ResidentLookup::Found(frame)
            }
            None => ResidentLookup::Absent,
        }
    }

    /// Run `attempt` under the pool lock. `Ok(Some)` succeeds; `Ok(None)` means the
    /// pool is full, so free one frame (flushing a WAL-durable dirty victim outside
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
    /// WAL-durable dirty victim (stealing enabled and the flush policy admits it)
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

            // Flush the reserved victim to its home WITHOUT holding the pool lock.
            // Safety rests on the per-frame pin/latch discipline plus the `evicting`
            // flag, not on any global controller (lock-free readers take no controller
            // guard; under E2b writers no longer serialize either):
            //
            // - A victim is reclaimed only when `pin_count == 0`, so no frame with an
            //   in-flight read/write (each pins via its live guard) is ever chosen.
            // - `reclaim_victim` set `evicting` under the pool lock at reservation, so
            //   from now until removal NO accessor can hand this frame out or modify it
            //   (`read`/`write`/`get_or_insert_clean` see `evicting` and retry). With
            //   `pin_count == 0` at reservation and no new pins afterward, the bytes are
            //   frozen — the snapshot below is a stable, consistent copy. This closes
            //   the lost-update race a concurrent writer could otherwise win (modify the
            //   frame after the snapshot, then the steal marks it clean and drops it),
            //   which the single global writer lock previously masked (pre-E2b).
            // - `ensure_durable` forces the WAL so the possibly-uncommitted page's
            //   records are durable before it reaches the heap (write-ahead logging,
            //   Milestone D1).
            //
            // Either fallible step (`ensure_durable` or the page write) must clear
            // `evicting` and release the reservation pin on error, or the victim frame
            // leaks (stays pinned + un-handout-able, never evictable).
            let flush_result = self.flush_policy.ensure_durable().and_then(|()| {
                let data = victim.data.read().clone();
                self.store
                    .write_page(victim.file_id, victim.page_num, &data)
            });
            if let Err(err) = flush_result {
                // Abort the eviction: clear `evicting` so the frame is usable again,
                // then release the reservation pin before propagating.
                victim.evicting.store(false, Ordering::Release);
                victim.unpin();
                return Err(err);
            }

            let mut state = self.state.lock();
            victim.unpin(); // release the flush reservation
            // With `evicting` set no accessor could re-pin the frame after reservation,
            // so `pin_count` is 0 here. Remove it. (The defensive re-check stays: were
            // it ever non-zero, abort the eviction rather than drop a referenced frame.)
            if victim.pin_count.load(Ordering::Acquire) == 0 {
                victim.mark_clean();
                state.remove_frame((victim.file_id, victim.page_num));
                return Ok(());
            }
            // Defensive: a frame somehow still referenced — abort this eviction and
            // try another. Clear `evicting` so it is usable again.
            victim.evicting.store(false, Ordering::Release);
        }
    }

    /// Seed the page allocator for `file_id` from its on-disk extent the first
    /// time the file is allocated into, so a freshly allocated page never reuses
    /// one that already exists on disk. Recovery no longer preloads pages, so
    /// without this the counter would start at 0 for checkpointed-but-not-replayed
    /// files and `new_page` would overwrite committed data.
    ///
    /// Concurrent-writer safety (Milestone E2b). The on-disk extent read and the
    /// seed are done under ONE continuous hold of the pool lock, so the
    /// "read `page_count` → `ensure_next_page_at_least`" pair is atomic against any
    /// concurrent pool-lock holder (another `new_page`/`load_page` advancing the
    /// counter, or a steal removing a frame). Seeding happens at most once per file
    /// (`extent_seeded`). Independently, the only on-disk extender of `file_id`
    /// besides this seed is (a) the checkpoint's `flush_dirty_pages`, which runs
    /// alone under the EXCLUSIVE guard so no writer — hence no seed — is concurrent,
    /// and (b) steal-eviction writing a stolen dirty page, whose page number was
    /// already allocated by a prior `new_page(file_id)` that already seeded
    /// `file_id` — so a steal of `file_id` implies `file_id` is already seeded and
    /// cannot grow it out from under a first seed. (Pre-E2b this read happened
    /// OUTSIDE the lock, justified only by the now-removed single global writer
    /// guard; the lock-held read makes the seed self-contained.)
    fn ensure_extent_seeded(&self, file_id: FileId) -> Result<()> {
        // Read the extent and seed under ONE continuous hold of the pool lock, so
        // the "read `page_count` → `ensure_next_page_at_least`" pair is atomic: no
        // concurrent pool-lock holder (another `new_page`/`load_page` advancing the
        // counter, or a steal removing a frame) can interleave between the read and
        // the seed and leave the counter seeded below the true extent. Seeding
        // happens at most once per file (`extent_seeded`), so the page-count syscall
        // under the lock runs at most once per file — cheap — and it cannot re-enter
        // the pool lock (it takes only the page store's own file-handle lock, never
        // this one).
        let mut state = self.state.lock();
        if state.extent_seeded.contains(&file_id) {
            return Ok(());
        }
        let extent = self.store.page_count(file_id)?;
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

    /// Classify a resident frame for a write, like `lookup_resident`, but marking the
    /// frame dirty under `txn_id` when found. An `evicting` frame is not handed out
    /// (the caller retries); a missing frame loads from the store.
    fn prepare_write_frame(
        &self,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
    ) -> ResidentLookup {
        let state = self.state.lock();
        match state.frames.get(&(file_id, page_num)) {
            Some(frame) if frame.evicting.load(Ordering::Acquire) => ResidentLookup::Evicting,
            Some(frame) => {
                let frame = frame.clone();
                frame.mark_dirty(txn_id);
                frame.pin();
                ResidentLookup::Found(frame)
            }
            None => ResidentLookup::Absent,
        }
    }

    fn storage_internal_error(message: impl Into<String>) -> DbError {
        DbError::storage(SqlState::InternalError, message)
    }
}

impl BufferPool for MemoryBufferPool {
    fn read_page(&self, file_id: FileId, page_num: PageNum) -> Result<PageReadGuard> {
        loop {
            match self.lookup_resident(file_id, page_num) {
                ResidentLookup::Found(frame) => return Ok(read_guard(file_id, page_num, frame)),
                // A steal is flushing this page; wait for it to finish (it removes the
                // frame under the lock), then load the flushed bytes from the store.
                ResidentLookup::Evicting => {
                    std::thread::yield_now();
                }
                ResidentLookup::Absent => {
                    return match self.store.load_page(file_id, page_num)? {
                        Some(data) => self.insert_loaded_read_page(file_id, page_num, data),
                        None => Err(Self::storage_internal_error(format!(
                            "page not found: file_id={file_id}, page_num={page_num}"
                        ))),
                    };
                }
            }
        }
    }

    fn write_page(
        &self,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
    ) -> Result<PageWriteGuard> {
        loop {
            match self.prepare_write_frame(file_id, page_num, txn_id) {
                ResidentLookup::Found(frame) => {
                    return Ok(write_guard(file_id, page_num, frame));
                }
                ResidentLookup::Evicting => {
                    std::thread::yield_now();
                }
                ResidentLookup::Absent => {
                    let frame = match self.store.load_page(file_id, page_num)? {
                        Some(data) => {
                            self.insert_loaded_write_page(file_id, page_num, txn_id, data)?
                        }
                        None => {
                            return Err(Self::storage_internal_error(format!(
                                "page not found: file_id={file_id}, page_num={page_num}"
                            )));
                        }
                    };
                    return Ok(write_guard(file_id, page_num, frame));
                }
            }
        }
    }

    fn new_page(&self, file_id: FileId, txn_id: u64) -> Result<PageWriteGuard> {
        self.ensure_extent_seeded(file_id)?;
        let (page_num, frame) = self.with_room(|state| {
            if state.frames.len() >= self.frame_count {
                return Ok(None);
            }
            let page_num = state
                .reusable_page(file_id)
                .unwrap_or_else(|| state.next_page_num(file_id));
            let frame = state.insert_fresh_frame(file_id, page_num)?;
            frame.mark_dirty(txn_id);
            state.mark_page_allocated(file_id, page_num);
            // Under status-based abort (`docs/specs/mvcc.md` §4 Decision 3) a
            // freshly allocated page is NOT reclaimed on rollback: it carries the
            // aborting transaction's (now-invisible) tuples and matching WAL records
            // that redo-all recovery replays, so dropping it at runtime would
            // diverge from the recovered state and dangle the index entries that
            // point at it. The page stays a normal dirty-but-evictable frame, hidden
            // by the CLOG. No per-transaction page bookkeeping is needed.
            frame.pin();
            Ok(Some((page_num, frame)))
        })?;
        Ok(new_page_write_guard(file_id, page_num, frame))
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

    fn page_count(&self, file_id: FileId) -> Result<PageNum> {
        // The on-disk extent (flushed pages) and the in-memory allocation counter
        // can disagree: a freshly allocated page is dirty-resident and not yet on
        // disk (so `store.page_count` lags), while after eviction the page exists
        // only on disk (so the on-disk extent leads). Take the max so the reported
        // extent covers every page that has ever existed for the file regardless of
        // where it currently lives. `next_page_num` is the next id to assign, i.e.
        // the count of allocated pages.
        let on_disk = self.store.page_count(file_id)?;
        let in_memory = self.state.lock().next_page_num(file_id);
        Ok(on_disk.max(in_memory))
    }

    fn abandon_unpublished_new_page(&self, guard: PageWriteGuard) -> Result<()> {
        let file_id = guard.file_id;
        let page_num = guard.page_num;
        if !guard.unpublished_new {
            return Err(Self::storage_internal_error(format!(
                "cannot abandon page that was not returned by new_page: file_id={file_id}, page_num={page_num}"
            )));
        }
        if guard.bytes_published {
            return Err(Self::storage_internal_error(format!(
                "cannot abandon page after mutable bytes were published: file_id={file_id}, page_num={page_num}"
            )));
        }
        let mut state = self.state.lock();
        let key = (file_id, page_num);
        let Some(frame) = state.frames.get(&key).cloned() else {
            return Err(Self::storage_internal_error(format!(
                "cannot abandon non-resident page: file_id={file_id}, page_num={page_num}"
            )));
        };
        let pins = frame.pin_count.load(Ordering::Acquire);
        if pins > 1 {
            return Err(Self::storage_internal_error(format!(
                "cannot abandon page with other pins: file_id={file_id}, page_num={page_num}"
            )));
        }
        state.remove_frame(key);
        state.abandon_allocated_page(file_id, page_num);
        Ok(())
    }

    fn is_page_abandoned(&self, file_id: FileId, page_num: PageNum) -> bool {
        self.state.lock().is_page_abandoned(file_id, page_num)
    }

    fn mark_all_clean(&self) -> Result<()> {
        let state = self.state.lock();
        for frame in state.frames.values() {
            frame.mark_clean();
        }
        Ok(())
    }

    fn rollback(&self, _txn_id: u64) -> Result<()> {
        // Status-based abort (`docs/specs/mvcc.md` §4 Decision 3, §11, Milestone
        // D1): there is NO page undo and NO page reclamation. An aborting
        // transaction's pages — both ones it modified in place and ones it freshly
        // allocated — stay resident as dirty-but-evictable frames. Their tuples are
        // hidden by the CLOG (`CLOG[txn] = Aborted`) and reclaimed by VACUUM
        // (Milestone F); redo-all recovery replays the same pages, so keeping them
        // at runtime matches the recovered state (and avoids dangling the index
        // entries that point at a freshly allocated page). No pins are leaked: the
        // statement's `PageWriteGuard`s were already dropped (unpinning their
        // frames) before this runs. The before-image mechanism earlier milestones
        // used is retired — it could not un-flush an already-evicted page and is
        // incompatible with the concurrent writers of Milestone E.
        Ok(())
    }

    fn commit(&self, _txn_id: u64) -> Result<()> {
        // Commit keeps the transaction's dirty pages resident for the next
        // checkpoint to flush; there is no per-transaction page bookkeeping to
        // clear (abort no longer reclaims pages, so none is tracked).
        Ok(())
    }

    fn enable_stealing(&self) {
        self.stealing.store(true, Ordering::Release);
    }

    fn flush_dirty_pages(&self) -> Result<()> {
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
            // Checkpoint runs under the exclusive write guard, after the WAL is
            // flushed, so every dirty page is WAL-durable and the relaxed policy
            // (`docs/specs/mvcc.md` §8, Milestone D1) admits it whether or not its
            // dirtying transaction committed — committed, aborted, and in-flight
            // pages all spill to the heap (the CLOG hides the non-committed ones).
            // An unflushable dirty page would be silently dropped by the subsequent
            // `mark_all_clean`, so fail loudly instead.
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
        // Recovery is single-threaded (no concurrent steal), so `Evicting` cannot
        // occur here; loop anyway to keep the contract uniform with `write_page`.
        loop {
            match self.prepare_write_frame(file_id, page_num, RECOVERY_TXN) {
                ResidentLookup::Found(frame) => {
                    return Ok(write_guard(file_id, page_num, frame));
                }
                ResidentLookup::Evicting => {
                    std::thread::yield_now();
                }
                ResidentLookup::Absent => {
                    let data = self.store.load_page(file_id, page_num)?.unwrap_or_default();
                    let frame =
                        self.insert_loaded_write_page(file_id, page_num, RECOVERY_TXN, data)?;
                    return Ok(write_guard(file_id, page_num, frame));
                }
            }
        }
    }
}

#[derive(Default)]
struct PoolState {
    frames: HashMap<PageKey, Arc<Frame>>,
    clock_order: Vec<PageKey>,
    clock_hand: usize,
    next_page_num_by_file: HashMap<FileId, PageNum>,
    abandoned_pages_by_file: HashMap<FileId, BTreeSet<PageNum>>,
    /// Files whose allocation counter has been seeded from the on-disk extent.
    extent_seeded: HashSet<FileId>,
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

    fn reusable_page(&self, file_id: FileId) -> Option<PageNum> {
        self.abandoned_pages_by_file
            .get(&file_id)
            .and_then(|pages| pages.iter().next().copied())
    }

    fn mark_page_allocated(&mut self, file_id: FileId, page_num: PageNum) {
        if let Some(pages) = self.abandoned_pages_by_file.get_mut(&file_id) {
            pages.remove(&page_num);
            if pages.is_empty() {
                self.abandoned_pages_by_file.remove(&file_id);
            }
        }
        self.advance_next_page_num(file_id, page_num);
    }

    fn abandon_allocated_page(&mut self, file_id: FileId, page_num: PageNum) {
        self.abandoned_pages_by_file
            .entry(file_id)
            .or_default()
            .insert(page_num);
        self.trim_abandoned_tail(file_id);
    }

    fn trim_abandoned_tail(&mut self, file_id: FileId) {
        while let Some(next) = self.next_page_num_by_file.get(&file_id).copied() {
            if next == 0 {
                break;
            }
            let tail = next - 1;
            let Some(pages) = self.abandoned_pages_by_file.get_mut(&file_id) else {
                break;
            };
            if !pages.remove(&tail) {
                break;
            }
            if pages.is_empty() {
                self.abandoned_pages_by_file.remove(&file_id);
            }
            if tail == 0 {
                self.next_page_num_by_file.remove(&file_id);
            } else {
                self.next_page_num_by_file.insert(file_id, tail);
            }
        }
    }

    fn is_page_abandoned(&self, file_id: FileId, page_num: PageNum) -> bool {
        self.abandoned_pages_by_file
            .get(&file_id)
            .is_some_and(|pages| pages.contains(&page_num))
    }

    /// Return the resident frame for `(file_id, page_num)`, or insert `data` as a
    /// clean frame if there is room. `None` means the pool is full **or** the resident
    /// frame is mid-eviction (`evicting`); either way the caller frees a frame / waits
    /// and retries (Milestone E2b). A resident page is returned unchanged (bytes,
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
            // A frame a steal is flushing must not be handed out (a writer could lose
            // its modification against the in-flight flush). Signal a retry; the steal
            // removes the frame shortly and the retry re-loads the flushed bytes.
            if frame.evicting.load(Ordering::Acquire) {
                return None;
            }
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

    /// Clock-sweep for an eviction victim. A clean unpinned frame is removed
    /// immediately (`FreedClean`). When stealing is enabled, a WAL-durable dirty
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
                    // Reserve across the unlocked flush. `pin_count == 0` here (checked
                    // above), so no accessor currently holds this frame; setting
                    // `evicting` under the pool lock now makes every subsequent
                    // resident-page lookup skip it, so no NEW accessor can grab it and
                    // modify its bytes while the steal flushes them. The pin keeps
                    // another steal from also reserving it.
                    frame.evicting.store(true, Ordering::Release);
                    frame.pin();
                    return ReclaimOutcome::ReservedDirty(frame);
                }
            }
            self.advance_clock_hand();
        }
        ReclaimOutcome::NoVictim
    }
}

/// Classification of a resident-page lookup under the pool lock (Milestone E2b).
enum ResidentLookup {
    /// The page is resident and usable; the frame is pinned and ready.
    Found(Arc<Frame>),
    /// A steal is flushing this exact page (`evicting`): the caller must drop the
    /// lock, yield, and retry, then load the flushed bytes from the store.
    Evicting,
    /// The page is not resident; load it from the store.
    Absent,
}

/// Outcome of a clock-sweep victim search (see `PoolState::reclaim_victim`).
enum ReclaimOutcome {
    /// A clean frame was removed under the lock; room is available.
    FreedClean,
    /// A WAL-durable dirty frame was pinned for an out-of-lock flush, then eviction.
    ReservedDirty(Arc<Frame>),
    /// No frame can be evicted (all pinned or unflushable dirty).
    NoVictim,
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
    /// Set under the pool lock when a steal reserves this dirty frame for an
    /// out-of-lock flush+evict (Milestone E2b). While set, no accessor may hand the
    /// frame out for use (`read`/`write`/`get_or_insert_clean` treat it as in
    /// transition and retry), so a concurrent writer can never modify a frame whose
    /// bytes the steal is flushing — closing the lost-update race the single global
    /// writer lock previously masked. Cleared if the eviction is aborted (the frame
    /// got re-pinned); a removed frame drops, so the flag need not be reset there.
    evicting: AtomicBool,
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
            evicting: AtomicBool::new(false),
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
        unpublished_new: false,
        bytes_published: false,
    }
}

fn new_page_write_guard(file_id: FileId, page_num: PageNum, frame: Arc<Frame>) -> PageWriteGuard {
    let guard = frame.data.write_arc();
    PageWriteGuard {
        file_id,
        page_num,
        frame,
        guard,
        unpublished_new: true,
        bytes_published: false,
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
    fn rollback_does_not_undo_in_place_modifications_and_leaves_them_dirty() {
        // Status-based abort (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1):
        // rollback no longer restores a before-image. A page the aborted txn merely
        // MODIFIED keeps its modified bytes and stays dirty-but-evictable; its
        // tuples are hidden by the CLOG (Aborted), not physically undone. (Before
        // D1 this test asserted the page was restored to the committed value 10;
        // updated to assert no-undo, which is the new abort contract.)
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

        // The last modification survives the rollback (no before-image undo).
        let page = pool.read_page(1, 0).unwrap();
        assert_eq!(page.data()[0], 30);
        drop(page);

        // The page is still resident and dirty, so it is evictable/flushable.
        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_dirty);
    }

    #[test]
    fn rollback_keeps_a_freshly_allocated_page_resident_and_dirty() {
        // Status-based abort no longer reclaims pages (`docs/specs/mvcc.md` §4
        // Decision 3, Milestone D1): a page the aborting txn freshly allocated stays
        // resident (its tuples hidden by the CLOG, its WAL records replayed by
        // recovery), as a dirty-but-evictable frame. (Before D1 this asserted the
        // page was removed; keeping it matches the recovered state and avoids
        // dangling index entries.)
        let pool = MemoryBufferPool::empty(8);

        {
            let mut page = pool.new_page(1, 77).unwrap();
            page.data_mut()[0] = 99;
        }

        pool.rollback(77).unwrap();

        // The page is still present, still dirty, and keeps its content (no undo).
        assert_eq!(pool.read_page(1, 0).unwrap().data()[0], 99);
        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_dirty);
    }

    #[test]
    fn rollback_does_not_reuse_allocated_page_numbers() {
        // The allocation counter is NOT reset on rollback (`docs/specs/mvcc.md` §4
        // Decision 3, Milestone D1): the freshly allocated pages survive (invisible
        // via the CLOG), so a later allocation must NOT reuse their page numbers and
        // overwrite them. (Before D1, rollback reclaimed the pages and reset the
        // counter so the numbers were reused; with no reclamation the counter only
        // advances.)
        let pool = MemoryBufferPool::empty(8);

        {
            let _meta = pool.new_page(1, 7).unwrap();
            let _root = pool.new_page(1, 7).unwrap();
        }
        pool.rollback(7).unwrap();

        // The next allocation gets a fresh page number (2), not a reused 0/1.
        let page = pool.new_page(1, 8).unwrap();
        assert_eq!(page.page_num(), 2);
    }

    #[test]
    fn abandon_unpublished_new_page_reuses_tail_page_number() {
        let pool = MemoryBufferPool::empty(8);

        let page = pool.new_page(1, 7).unwrap();
        let page_num = page.page_num();
        pool.abandon_unpublished_new_page(page).unwrap();

        assert_eq!(pool.page_count(1).unwrap(), 0);
        assert!(!pool.is_page_abandoned(1, page_num));

        let reused = pool.new_page(1, 8).unwrap();
        assert_eq!(reused.page_num(), page_num);
    }

    #[test]
    fn abandon_unpublished_new_page_reuses_interior_hole_before_growing_extent() {
        let pool = MemoryBufferPool::empty(8);

        let page0 = pool.new_page(1, 7).unwrap();
        let page0_num = page0.page_num();
        let page1 = pool.new_page(1, 7).unwrap();
        assert_eq!(page1.page_num(), page0_num + 1);

        pool.abandon_unpublished_new_page(page0).unwrap();
        assert_eq!(pool.page_count(1).unwrap(), 2);
        assert!(pool.is_page_abandoned(1, page0_num));

        let reused = pool.new_page(1, 8).unwrap();
        assert_eq!(reused.page_num(), page0_num);
        assert!(!pool.is_page_abandoned(1, page0_num));
        assert_eq!(pool.page_count(1).unwrap(), 2);
    }

    #[test]
    fn abandon_unpublished_new_page_rejects_after_bytes_are_published() {
        let pool = MemoryBufferPool::empty(8);

        let mut page = pool.new_page(1, 7).unwrap();
        let page_num = page.page_num();
        page.data_mut()[0] = 42;

        let err = pool.abandon_unpublished_new_page(page).unwrap_err();
        assert!(err.message.contains("mutable bytes were published"));
        assert_eq!(pool.read_page(1, page_num).unwrap().data()[0], 42);
        assert!(!pool.is_page_abandoned(1, page_num));
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
    fn rollback_of_modified_page_leaves_it_dirty_without_undo() {
        // A modified (previously-loaded) page is NOT restored by rollback under the
        // status-based abort (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1): the
        // modified bytes remain and the page stays dirty. (Before D1 this asserted
        // the rollback cleaned the page so it became evictable; updated to the
        // no-undo contract — an aborted-but-flushable dirty page is the new normal,
        // hidden by the CLOG rather than physically reverted.)
        let pool = MemoryBufferPool::empty(2);
        pool.load_page(1, 0, data_with_first_byte(1)).unwrap();
        {
            let mut page = pool.write_page(1, 0, 1).unwrap();
            page.data_mut()[0] = 9;
        }

        pool.rollback(1).unwrap();

        // No before-image undo: the modified value 9 survives, and the page is dirty.
        assert_eq!(pool.read_page(1, 0).unwrap().data()[0], 9);
        let pages: Vec<_> = pool.iter_pages().unwrap().collect();
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_dirty);
    }

    #[test]
    fn load_page_advances_next_page_number() {
        let pool = MemoryBufferPool::empty(8);
        pool.load_page(7, 3, PageData::default()).unwrap();

        let page = pool.new_page(7, 1).unwrap();

        assert_eq!(page.page_num(), 4);
    }

    #[test]
    fn page_count_is_the_max_of_disk_extent_and_in_memory_allocation() {
        // Three pages live on disk (the store reports extent 3); none are resident.
        let loader = Arc::new(TestPageLoader::new([
            ((7, 0), PageData::default()),
            ((7, 1), PageData::default()),
            ((7, 2), PageData::default()),
        ]));
        let pool = MemoryBufferPool::new(8, Box::new(NeverFlush), loader);

        // Before any allocation the full extent is the on-disk count, even though no
        // page of file 7 is resident — this is what a full-extent VACUUM scan needs.
        assert_eq!(pool.page_count(7).unwrap(), 3);

        // Allocating a fresh (in-memory, not-yet-flushed) page extends the count past
        // the on-disk extent: page_count must include it so the scan visits it.
        let allocated = pool.new_page(7, 1).unwrap();
        assert_eq!(allocated.page_num(), 3);
        assert_eq!(pool.page_count(7).unwrap(), 4);

        // A different, never-touched file has an empty extent.
        assert_eq!(pool.page_count(99).unwrap(), 0);
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
        // Status-based abort does no before-image undo (`docs/specs/mvcc.md` §4
        // Decision 3, Milestone D1): the modified value 9 survives the rollback.
        // (Before D1 the page was restored to its loaded value 1.)
        pool.rollback(77).unwrap();
        assert_eq!(pool.read_page(1, 0).unwrap().data()[0], 9);
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

        // The loaded-then-modified page was not freshly allocated by this txn, so a
        // status-based rollback leaves its (modified) content in place — no
        // before-image undo (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1).
        // (Before D1 the rollback restored the loader value 88.)
        pool.rollback(99).unwrap();
        let page = pool.read_page(2, 5).unwrap();
        assert_eq!(page.data()[0], 99);
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
    fn flush_dirty_pages_writes_dirty_pages_to_store() {
        let store = Arc::new(CapturingStore::default());
        let pool = MemoryBufferPool::new(8, Box::new(FlushAll), store.clone());
        {
            let mut page = pool.new_page(1, 5).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(5).unwrap();

        pool.flush_dirty_pages().unwrap();

        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 1);
        assert_eq!(writes[0].1, 0);
        assert_eq!(writes[0].2.0[0], 42);
    }

    #[test]
    fn flush_dirty_pages_writes_an_aborted_txns_dirty_page() {
        // The relaxed flush gate (`docs/specs/mvcc.md` §8, Milestone D1) admits a
        // page dirtied by an aborted transaction: checkpoint spills it to the heap
        // (the CLOG hides its tuples). `FlushAll` models the WAL-durable policy.
        let store = Arc::new(CapturingStore::default());
        let pool = MemoryBufferPool::new(8, Box::new(FlushAll), store.clone());
        {
            let mut page = pool.new_page(1, 7).unwrap();
            page.data_mut()[0] = 9;
        }
        // The txn aborts (status-based: its allocated page is dropped on rollback),
        // so instead model an in-place modification of a committed page that then
        // aborts: it stays dirty and must still be flushed by a checkpoint.
        pool.commit(7).unwrap();
        {
            let mut page = pool.write_page(1, 0, 8).unwrap();
            page.data_mut()[0] = 99;
        }
        pool.rollback(8).unwrap();

        pool.flush_dirty_pages().unwrap();

        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].2.0[0], 99, "aborted txn's modified page spills");
    }

    #[test]
    fn flush_dirty_pages_errors_on_unflushable_dirty_page() {
        let store = Arc::new(CapturingStore::default());
        let pool = MemoryBufferPool::new(8, Box::new(NeverFlush), store.clone());
        {
            let mut page = pool.new_page(1, 5).unwrap();
            page.data_mut()[0] = 42;
        }
        pool.commit(5).unwrap();

        // A dirty page that the policy refuses (not WAL-durable) must fail loudly,
        // never be silently dropped (it would be lost by the subsequent
        // mark_all_clean).
        let err = pool.flush_dirty_pages().unwrap_err();
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
    fn stealing_flushes_wal_durable_dirty_page_on_eviction() {
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

    /// Concurrent steal-vs-write regression (Milestone E2b). Several threads each
    /// allocate fresh pages and stamp a unique byte into each through a TINY pool, so
    /// most pages are continuously stolen out to the store while others allocate and
    /// write. Every page's stamped byte must survive — read back from the pool (which
    /// reloads stolen pages from the store). Before the `evicting`-flag guard a steal
    /// could flush a stale snapshot of a frame a writer was concurrently modifying and
    /// then drop the frame, silently losing the write; this test would then read back
    /// the wrong byte (or a missing page).
    #[test]
    fn concurrent_writes_survive_steal_eviction() {
        use std::sync::Barrier;
        use std::thread;

        let store = Arc::new(MemStore::default());
        // A small pool with just a little headroom over the concurrent in-flight set
        // (one pinned write page per thread): enough that `new_page` always finds a
        // victim, but small enough that nearly every page is stolen out to the store —
        // maximizing the steal-vs-write overlap the `evicting` guard must make safe.
        const THREADS: u32 = 4;
        const PER_THREAD: u32 = 80;
        let pool = Arc::new(MemoryBufferPool::new(
            THREADS as usize + 2,
            Box::new(FlushAll),
            store.clone(),
        ));
        pool.enable_stealing();
        let barrier = Arc::new(Barrier::new(THREADS as usize));
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let pool = pool.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                let file_id: FileId = t; // disjoint file per thread (distinct page space)
                let txn = u64::from(t) + 1;
                barrier.wait();
                let mut pages = Vec::new();
                for i in 0..PER_THREAD {
                    let mut page = pool.new_page(file_id, txn).unwrap();
                    // A byte pattern unique to (thread, sequence) so a lost/overwritten
                    // write is detectable on read-back.
                    let stamp = ((t << 4) ^ i) as u8;
                    page.data_mut()[0] = stamp;
                    page.data_mut()[1] = t as u8;
                    let page_num = page.page_num();
                    drop(page); // unpin so the frame is steal-eligible
                    pages.push((page_num, stamp));
                }
                pool.commit(txn).unwrap();
                (file_id, pages)
            }));
        }

        let mut all = Vec::new();
        for handle in handles {
            all.push(handle.join().expect("writer thread finished"));
        }

        // Every page's stamp survives (read back, reloading stolen pages from store).
        for (file_id, pages) in all {
            for (page_num, stamp) in pages {
                let page = pool.read_page(file_id, page_num).unwrap();
                assert_eq!(
                    page.data()[0],
                    stamp,
                    "page {file_id}/{page_num} lost its concurrently-written byte to a steal"
                );
            }
        }
    }
}
