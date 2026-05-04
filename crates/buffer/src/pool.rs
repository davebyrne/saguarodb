use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use common::{DbError, FileId, FlushPolicy, PageFlushInfo, PageNum, Result, SqlState};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, Mutex, RawRwLock, RwLock};

use crate::{PAGE_SIZE, PageData, PageInfo, PageLoader};

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
}

pub struct MemoryBufferPool {
    frame_count: usize,
    _flush_policy: Box<dyn FlushPolicy>,
    page_loader: Arc<dyn PageLoader>,
    state: Mutex<PoolState>,
}

impl MemoryBufferPool {
    pub fn new(
        frame_count: usize,
        flush_policy: Box<dyn FlushPolicy>,
        page_loader: Arc<dyn PageLoader>,
    ) -> Self {
        Self {
            frame_count,
            _flush_policy: flush_policy,
            page_loader,
            state: Mutex::new(PoolState::default()),
        }
    }

    pub fn empty(frame_count: usize) -> Self {
        Self::new(frame_count, Box::new(NeverFlush), Arc::new(NoopPageLoader))
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

    fn insert_loaded_read_page(
        &self,
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
    ) -> Result<PageReadGuard> {
        let frame = {
            let mut state = self.state.lock();
            let frame = state.insert_frame_if_absent(self.frame_count, file_id, page_num, data)?;
            state.advance_next_page_num(file_id, page_num);
            frame.pin();
            frame
        };
        Ok(read_guard(file_id, page_num, frame))
    }

    fn insert_loaded_write_page(
        &self,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
        data: PageData,
    ) -> Result<Arc<Frame>> {
        let mut state = self.state.lock();
        let frame = state.insert_frame_if_absent(self.frame_count, file_id, page_num, data)?;
        state.advance_next_page_num(file_id, page_num);
        state.record_before_image(txn_id, file_id, page_num, &frame);
        frame.mark_dirty(txn_id);
        frame.pin();
        Ok(frame)
    }

    fn insert_clean_page_if_absent(
        &self,
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
    ) -> Result<()> {
        let mut state = self.state.lock();
        state.insert_frame_if_absent(self.frame_count, file_id, page_num, data)?;
        state.advance_next_page_num(file_id, page_num);
        Ok(())
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

        match self.page_loader.load_page(file_id, page_num)? {
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
            match self.page_loader.load_page(file_id, page_num)? {
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
        let (page_num, frame) = {
            let mut state = self.state.lock();
            let page_num = state.next_page_num(file_id);
            let frame = state.insert_frame(
                self.frame_count,
                file_id,
                page_num,
                PageData::default(),
                true,
            )?;
            frame.mark_dirty(txn_id);
            state.advance_next_page_num(file_id, page_num);
            state
                .txns
                .entry(txn_id)
                .or_default()
                .new_pages
                .push((file_id, page_num));
            frame.pin();
            (page_num, frame)
        };
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
}

#[derive(Default)]
struct PoolState {
    frames: HashMap<PageKey, Arc<Frame>>,
    clock_order: Vec<PageKey>,
    clock_hand: usize,
    next_page_num_by_file: HashMap<FileId, PageNum>,
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

    fn insert_frame(
        &mut self,
        frame_count: usize,
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
        dirty: bool,
    ) -> Result<Arc<Frame>> {
        let key = (file_id, page_num);
        if let Some(frame) = self.frames.get(&key) {
            *frame.data.write() = data;
            if dirty {
                frame.dirty.store(true, Ordering::Release);
            } else {
                frame.mark_clean();
            }
            frame.reference_bit.store(true, Ordering::Release);
            return Ok(frame.clone());
        }

        if frame_count == 0 {
            return Err(MemoryBufferPool::storage_internal_error(
                "buffer pool has zero frames",
            ));
        }

        if self.frames.len() >= frame_count {
            self.evict_one_clean_unpinned()?;
        }

        let frame = Arc::new(Frame::new(file_id, page_num, data, dirty));
        self.frames.insert(key, frame.clone());
        self.clock_order.push(key);
        Ok(frame)
    }

    fn insert_frame_if_absent(
        &mut self,
        frame_count: usize,
        file_id: FileId,
        page_num: PageNum,
        data: PageData,
    ) -> Result<Arc<Frame>> {
        let key = (file_id, page_num);
        if let Some(frame) = self.frames.get(&key) {
            frame.reference_bit.store(true, Ordering::Release);
            return Ok(frame.clone());
        }

        if frame_count == 0 {
            return Err(MemoryBufferPool::storage_internal_error(
                "buffer pool has zero frames",
            ));
        }

        if self.frames.len() >= frame_count {
            self.evict_one_clean_unpinned()?;
        }

        let frame = Arc::new(Frame::new(file_id, page_num, data, false));
        self.frames.insert(key, frame.clone());
        self.clock_order.push(key);
        Ok(frame)
    }

    fn remove_frame(&mut self, key: PageKey) {
        self.frames.remove(&key);
        self.clock_order.retain(|candidate| *candidate != key);
        if !self.clock_order.is_empty() {
            self.clock_hand %= self.clock_order.len();
        } else {
            self.clock_hand = 0;
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

    fn evict_one_clean_unpinned(&mut self) -> Result<()> {
        let candidate_count = self.clock_order.len();
        for _ in 0..candidate_count.saturating_mul(2) {
            if self.clock_order.is_empty() {
                break;
            }

            self.clock_hand %= self.clock_order.len();
            let key = self.clock_order[self.clock_hand];
            let Some(frame) = self.frames.get(&key) else {
                self.clock_order.remove(self.clock_hand);
                continue;
            };

            if frame.is_evictable() {
                if frame.reference_bit.swap(false, Ordering::AcqRel) {
                    self.clock_hand = (self.clock_hand + 1) % self.clock_order.len();
                    continue;
                }

                self.frames.remove(&key);
                self.clock_order.remove(self.clock_hand);
                if !self.clock_order.is_empty() {
                    self.clock_hand %= self.clock_order.len();
                } else {
                    self.clock_hand = 0;
                }
                return Ok(());
            }

            self.clock_hand = (self.clock_hand + 1) % self.clock_order.len();
        }

        Err(MemoryBufferPool::storage_internal_error(
            "no clean unpinned frame available for eviction",
        ))
    }
}

#[derive(Default)]
struct TxnDirtyState {
    before_images: HashMap<PageKey, BeforeImage>,
    new_pages: Vec<PageKey>,
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
    dirty_since_snapshot: AtomicBool,
    dirty_txn_id: AtomicU64,
    reference_bit: AtomicBool,
}

impl Frame {
    fn new(file_id: FileId, page_num: PageNum, data: PageData, dirty: bool) -> Self {
        Self {
            file_id,
            page_num,
            data: Arc::new(RwLock::new(data)),
            pin_count: AtomicUsize::new(0),
            dirty: AtomicBool::new(dirty),
            dirty_since_snapshot: AtomicBool::new(dirty),
            dirty_txn_id: AtomicU64::new(0),
            reference_bit: AtomicBool::new(true),
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
        self.dirty_since_snapshot.store(true, Ordering::Release);
        self.dirty_txn_id.store(txn_id, Ordering::Release);
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
        self.dirty_since_snapshot.store(false, Ordering::Release);
        self.dirty_txn_id.store(0, Ordering::Release);
    }

    fn restore_dirty_state(&self, was_dirty: bool, dirty_txn_id: u64) {
        self.dirty.store(was_dirty, Ordering::Release);
        self.dirty_since_snapshot
            .store(was_dirty, Ordering::Release);
        self.dirty_txn_id.store(dirty_txn_id, Ordering::Release);
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn is_evictable(&self) -> bool {
        !self.is_dirty() && self.pin_count.load(Ordering::Acquire) == 0
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

struct NoopPageLoader;

impl PageLoader for NoopPageLoader {
    fn load_page(&self, _file_id: FileId, _page_num: PageNum) -> Result<Option<PageData>> {
        Ok(None)
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
}
