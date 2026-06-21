//! On-disk, non-clustered primary-key index: a B+-tree of `Key -> RowLocation`
//! living in its own file, separate from the table heap. Rows stay in the heap;
//! this tree replaces the in-memory primary-key directory.
//!
//! Page 0 is the metapage (holds the root page number); other pages are leaf or
//! internal nodes (`index_page`). Leaves are singly linked left-to-right for
//! range scans. Insert splits nodes; delete removes the entry without merging
//! (accepted bloat). Every node mutation logs a `FullPageImage` and stamps the
//! page-LSN, so the tree is crash-safe through the same redo path as the heap.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Bound;

use buffer::{BufferPool, PageWriteGuard};
use common::{DbError, FileId, Key, KeyRange, PageNum, Result, SqlState};
use wal::{WalManager, WalRecord, WalRecordKind};

use crate::codec::{decode_key, encode_key};
use crate::engine::RowLocation;
use crate::index_page;

const META_PAGE: PageNum = 0;
const LOCATION_LEN: usize = 10;
const CHILD_LEN: usize = 4;

/// A value stored in a B-tree leaf. The primary-key index stores a `RowLocation`
/// (fixed width); a secondary index stores the row's primary `Key` (variable
/// width). The tree itself treats values as opaque bytes; this trait is the only
/// place a value's on-page encoding is defined.
pub(crate) trait IndexValue: Sized {
    fn encode(&self) -> Result<Vec<u8>>;
    fn decode(bytes: &[u8]) -> Result<Self>;
}

impl IndexValue for RowLocation {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(LOCATION_LEN);
        bytes.extend_from_slice(&self.file_id.to_le_bytes());
        bytes.extend_from_slice(&self.page_num.to_le_bytes());
        bytes.extend_from_slice(&self.slot_num.to_le_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != LOCATION_LEN {
            return Err(corrupt("index leaf value is not a row location"));
        }
        Ok(RowLocation {
            file_id: u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")),
            page_num: u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")),
            slot_num: u16::from_le_bytes(bytes[8..10].try_into().expect("2 bytes")),
        })
    }
}

impl IndexValue for Key {
    fn encode(&self) -> Result<Vec<u8>> {
        encode_key(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_key(bytes)
    }
}

/// A B+-tree over one index file, generic over its leaf value type `V`. Reads
/// need only the buffer pool; mutations also log redo through the WAL under the
/// statement's `txn_id`.
pub(crate) struct BTree<'a, V> {
    buffer: &'a dyn BufferPool,
    wal: &'a dyn WalManager,
    file_id: FileId,
    _value: PhantomData<fn() -> V>,
}

enum InsertOutcome {
    Inserted,
    Duplicate,
    Split {
        sep_key: Vec<u8>,
        right_page: PageNum,
    },
}

impl<'a, V: IndexValue> BTree<'a, V> {
    pub(crate) fn new(
        buffer: &'a dyn BufferPool,
        wal: &'a dyn WalManager,
        file_id: FileId,
    ) -> Self {
        Self {
            buffer,
            wal,
            file_id,
            _value: PhantomData,
        }
    }

    /// Create an empty index: a metapage (page 0) pointing at a fresh empty root
    /// leaf (page 1).
    pub(crate) fn create(&self, txn_id: u64) -> Result<()> {
        let mut meta = self.buffer.new_page(self.file_id, txn_id)?;
        let meta_num = meta.page_num();
        let mut root = self.buffer.new_page(self.file_id, txn_id)?;
        let root_num = root.page_num();

        index_page::init(root.data_mut(), root_num, true);
        self.log_full_page(txn_id, &mut root)?;

        index_page::meta_init(meta.data_mut(), meta_num, root_num);
        self.log_full_page(txn_id, &mut meta)?;
        Ok(())
    }

    pub(crate) fn search(&self, key: &Key) -> Result<Option<V>> {
        let mut page_num = self.root()?;
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            if index_page::is_leaf(data) {
                return match self.find_in_node(data, key)? {
                    Some(pos) => Ok(Some(V::decode(index_page::entry_value(data, pos))?)),
                    None => Ok(None),
                };
            }
            page_num = self.child_for(data, key)?;
        }
    }

    /// Insert `key -> value`. Returns `false` (and changes nothing) if the key
    /// already exists.
    pub(crate) fn insert(&self, txn_id: u64, key: &Key, value: &V) -> Result<bool> {
        let key_bytes = encode_key(key)?;
        let value = value.encode()?;
        let root = self.root()?;
        match self.insert_rec(txn_id, root, key, &key_bytes, &value)? {
            InsertOutcome::Inserted => Ok(true),
            InsertOutcome::Duplicate => Ok(false),
            InsertOutcome::Split {
                sep_key,
                right_page,
            } => {
                // The root split: grow the tree by one level with a new internal
                // root whose leftmost child is the old root.
                let mut new_root = self.buffer.new_page(self.file_id, txn_id)?;
                let new_root_num = new_root.page_num();
                index_page::init(new_root.data_mut(), new_root_num, false);
                index_page::set_link(new_root.data_mut(), root);
                index_page::insert_entry(
                    new_root.data_mut(),
                    0,
                    &sep_key,
                    &encode_child(right_page),
                )?;
                self.log_full_page(txn_id, &mut new_root)?;
                self.set_root(txn_id, new_root_num)?;
                Ok(true)
            }
        }
    }

    /// Remove `key`. Returns `false` if it was not present. Underfull nodes are
    /// left in place (no merge).
    pub(crate) fn remove(&self, txn_id: u64, key: &Key) -> Result<bool> {
        let mut page_num = self.root()?;
        loop {
            if self.node_is_leaf(page_num)? {
                let mut guard = self.buffer.write_page(self.file_id, page_num, txn_id)?;
                let Some(pos) = self.find_in_node(guard.data(), key)? else {
                    return Ok(false);
                };
                index_page::remove_entry(guard.data_mut(), pos)?;
                self.log_full_page(txn_id, &mut guard)?;
                return Ok(true);
            }
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            page_num = self.child_for(guard.data(), key)?;
        }
    }

    /// Update the value stored for `key` in place. Returns `false` if absent. The
    /// new value must encode to the same length as the old one (true for the
    /// fixed-width `RowLocation` of the primary-key index).
    pub(crate) fn update(&self, txn_id: u64, key: &Key, value: &V) -> Result<bool> {
        let mut page_num = self.root()?;
        loop {
            if self.node_is_leaf(page_num)? {
                let mut guard = self.buffer.write_page(self.file_id, page_num, txn_id)?;
                let Some(pos) = self.find_in_node(guard.data(), key)? else {
                    return Ok(false);
                };
                index_page::set_value(guard.data_mut(), pos, &value.encode()?)?;
                self.log_full_page(txn_id, &mut guard)?;
                return Ok(true);
            }
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            page_num = self.child_for(guard.data(), key)?;
        }
    }

    /// Collect `(key, value)` for every entry within `range`, in key order.
    ///
    /// Comparison uses only the leading components of each key that the range's
    /// bounds constrain (their length). For the primary-key index the bounds are
    /// full keys, so this is an exact-key range. For a secondary index the bounds
    /// constrain the indexed columns while each stored key is `[indexed.., pk]`,
    /// so the trailing primary key is ignored — an equality bound matches every
    /// row sharing the indexed value. Entries are returned with their full key.
    pub(crate) fn range(&self, range: &KeyRange) -> Result<Vec<(Key, V)>> {
        let prefix_len = comparison_prefix_len(range);
        let mut page_num = self.start_leaf(range)?;
        let mut out = Vec::new();
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            for pos in 0..index_page::entry_count(data) {
                let full = decode_key(index_page::entry_key(data, pos))?;
                let probe = prefix_of(&full, prefix_len);
                let compared = probe.as_ref().unwrap_or(&full);
                if beyond_end(compared, range) {
                    return Ok(out);
                }
                if key_in_range(compared, range) {
                    out.push((full, V::decode(index_page::entry_value(data, pos))?));
                }
            }
            let next = index_page::link(data);
            if next == 0 {
                return Ok(out);
            }
            page_num = next;
        }
    }

    fn insert_rec(
        &self,
        txn_id: u64,
        page_num: PageNum,
        key: &Key,
        key_bytes: &[u8],
        value: &[u8],
    ) -> Result<InsertOutcome> {
        if self.node_is_leaf(page_num)? {
            let mut guard = self.buffer.write_page(self.file_id, page_num, txn_id)?;
            let (pos, found) = self.position_in_node(guard.data(), key)?;
            if found {
                return Ok(InsertOutcome::Duplicate);
            }
            if index_page::has_space(guard.data(), key_bytes.len(), value.len()) {
                index_page::insert_entry(guard.data_mut(), pos, key_bytes, value)?;
                self.log_full_page(txn_id, &mut guard)?;
                return Ok(InsertOutcome::Inserted);
            }
            return self.split_node(txn_id, &mut guard, pos, key_bytes, value, true);
        }

        let child = {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            self.child_for(guard.data(), key)?
        };
        match self.insert_rec(txn_id, child, key, key_bytes, value)? {
            InsertOutcome::Split {
                sep_key,
                right_page,
            } => {
                let mut guard = self.buffer.write_page(self.file_id, page_num, txn_id)?;
                let sep = decode_key(&sep_key)?;
                let (pos, _) = self.position_in_node(guard.data(), &sep)?;
                let child_bytes = encode_child(right_page);
                if index_page::has_space(guard.data(), sep_key.len(), child_bytes.len()) {
                    index_page::insert_entry(guard.data_mut(), pos, &sep_key, &child_bytes)?;
                    self.log_full_page(txn_id, &mut guard)?;
                    Ok(InsertOutcome::Inserted)
                } else {
                    self.split_node(txn_id, &mut guard, pos, &sep_key, &child_bytes, false)
                }
            }
            outcome => Ok(outcome),
        }
    }

    /// Split a full node, placing the new entry at `pos`, and return the
    /// separator pushed to the parent plus the new right page. For a leaf the
    /// separator is a copy of the right half's first key; for an internal node
    /// the middle entry moves up and its child becomes the right node's leftmost.
    fn split_node(
        &self,
        txn_id: u64,
        guard: &mut PageWriteGuard,
        pos: u16,
        key_bytes: &[u8],
        value: &[u8],
        leaf: bool,
    ) -> Result<InsertOutcome> {
        let entries = entries_with_insertion(guard.data(), pos, key_bytes, value);
        let leftmost = index_page::link(guard.data());
        let old_right_link = leftmost; // for a leaf, link is the right sibling

        let mut right = self.buffer.new_page(self.file_id, txn_id)?;
        let right_num = right.page_num();
        let page_num = guard.page_num();
        let mid = split_point(&entries);

        if leaf {
            index_page::init(right.data_mut(), right_num, true);
            append_entries(right.data_mut(), &entries[mid..])?;
            index_page::set_link(right.data_mut(), old_right_link);
            self.log_full_page(txn_id, &mut right)?;

            index_page::init(guard.data_mut(), page_num, true);
            append_entries(guard.data_mut(), &entries[..mid])?;
            index_page::set_link(guard.data_mut(), right_num);
            self.log_full_page(txn_id, guard)?;

            Ok(InsertOutcome::Split {
                sep_key: entries[mid].0.clone(),
                right_page: right_num,
            })
        } else {
            let push_key = entries[mid].0.clone();
            let right_leftmost = decode_child(&entries[mid].1)?;

            index_page::init(right.data_mut(), right_num, false);
            index_page::set_link(right.data_mut(), right_leftmost);
            append_entries(right.data_mut(), &entries[mid + 1..])?;
            self.log_full_page(txn_id, &mut right)?;

            index_page::init(guard.data_mut(), page_num, false);
            index_page::set_link(guard.data_mut(), leftmost);
            append_entries(guard.data_mut(), &entries[..mid])?;
            self.log_full_page(txn_id, guard)?;

            Ok(InsertOutcome::Split {
                sep_key: push_key,
                right_page: right_num,
            })
        }
    }

    fn root(&self) -> Result<PageNum> {
        let guard = self.buffer.read_page(self.file_id, META_PAGE)?;
        Ok(index_page::meta_root(guard.data()))
    }

    fn set_root(&self, txn_id: u64, root: PageNum) -> Result<()> {
        let mut guard = self.buffer.write_page(self.file_id, META_PAGE, txn_id)?;
        index_page::meta_set_root(guard.data_mut(), root);
        self.log_full_page(txn_id, &mut guard)
    }

    fn node_is_leaf(&self, page_num: PageNum) -> Result<bool> {
        let guard = self.buffer.read_page(self.file_id, page_num)?;
        Ok(index_page::is_leaf(guard.data()))
    }

    /// Exact-match position of `key` in a node (leaf lookup / dedup).
    fn find_in_node(&self, data: &[u8; buffer::PAGE_SIZE], key: &Key) -> Result<Option<u16>> {
        let (pos, found) = self.position_in_node(data, key)?;
        Ok(found.then_some(pos))
    }

    /// The insertion position for `key` among a node's sorted entries, and whether
    /// an exact match already exists there.
    fn position_in_node(&self, data: &[u8; buffer::PAGE_SIZE], key: &Key) -> Result<(u16, bool)> {
        let count = index_page::entry_count(data);
        for pos in 0..count {
            let entry = decode_key(index_page::entry_key(data, pos))?;
            match entry.cmp(key) {
                Ordering::Less => {}
                Ordering::Equal => return Ok((pos, true)),
                Ordering::Greater => return Ok((pos, false)),
            }
        }
        Ok((count, false))
    }

    /// The child subtree of an internal node that may contain `key`.
    fn child_for(&self, data: &[u8; buffer::PAGE_SIZE], key: &Key) -> Result<PageNum> {
        let mut child = index_page::link(data); // leftmost child
        for pos in 0..index_page::entry_count(data) {
            let sep = decode_key(index_page::entry_key(data, pos))?;
            if sep.cmp(key) == Ordering::Greater {
                break;
            }
            child = decode_child(index_page::entry_value(data, pos))?;
        }
        Ok(child)
    }

    fn start_leaf(&self, range: &KeyRange) -> Result<PageNum> {
        let start = range_start_key(range);
        let mut page_num = self.root()?;
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            if index_page::is_leaf(data) {
                return Ok(page_num);
            }
            page_num = match &start {
                Some(key) => self.child_for(data, key)?,
                None => index_page::link(data),
            };
        }
    }

    fn log_full_page(&self, txn_id: u64, guard: &mut PageWriteGuard) -> Result<()> {
        let lsn = self.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::FullPageImage {
                file_id: self.file_id,
                page_num: guard.page_num(),
                image: guard.data().to_vec(),
            },
        })?;
        crate::page::set_page_lsn(guard.data_mut(), lsn);
        Ok(())
    }
}

/// All of a node's entries with `(key_bytes, value)` inserted at `pos`, owned so
/// the page can be rebuilt for a split.
fn entries_with_insertion(
    data: &[u8; buffer::PAGE_SIZE],
    pos: u16,
    key_bytes: &[u8],
    value: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let count = index_page::entry_count(data);
    let mut entries = Vec::with_capacity(count as usize + 1);
    for index in 0..count {
        if index == pos {
            entries.push((key_bytes.to_vec(), value.to_vec()));
        }
        entries.push((
            index_page::entry_key(data, index).to_vec(),
            index_page::entry_value(data, index).to_vec(),
        ));
    }
    if pos >= count {
        entries.push((key_bytes.to_vec(), value.to_vec()));
    }
    entries
}

fn append_entries(
    data: &mut [u8; buffer::PAGE_SIZE],
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<()> {
    for (index, (key, value)) in entries.iter().enumerate() {
        index_page::insert_entry(data, index as u16, key, value)?;
    }
    Ok(())
}

/// The number of entries the left node keeps after a split, chosen so each side
/// holds roughly half the bytes. Balancing by bytes (not entry count) keeps a
/// variable-length-key half from overflowing the page. Clamped so the left keeps
/// at least one entry and the right is non-empty for a leaf.
fn split_point(entries: &[(Vec<u8>, Vec<u8>)]) -> usize {
    let total: usize = entries
        .iter()
        .map(|(key, value)| index_page::entry_stored_len(key.len(), value.len()))
        .sum();
    let mut acc = 0;
    let mut mid = entries.len();
    for (index, (key, value)) in entries.iter().enumerate() {
        acc += index_page::entry_stored_len(key.len(), value.len());
        if acc * 2 >= total {
            mid = index + 1;
            break;
        }
    }
    mid.clamp(1, entries.len() - 1)
}

fn encode_child(page: PageNum) -> [u8; CHILD_LEN] {
    page.to_le_bytes()
}

fn decode_child(bytes: &[u8]) -> Result<PageNum> {
    if bytes.len() != CHILD_LEN {
        return Err(corrupt("index internal value is not a child pointer"));
    }
    Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")))
}

fn range_start_key(range: &KeyRange) -> Option<Key> {
    match range {
        KeyRange::All => None,
        KeyRange::Exact(key) => Some(key.clone()),
        KeyRange::Range { start, .. } => match start {
            Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
            Bound::Unbounded => None,
        },
    }
}

/// How many leading key components the range's bounds constrain. The bound keys
/// hold exactly the constrained columns, so their length is the comparison
/// prefix. An unbounded range constrains nothing.
fn comparison_prefix_len(range: &KeyRange) -> usize {
    let bound_len = |bound: &Bound<Key>| match bound {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.0.len()),
        Bound::Unbounded => None,
    };
    match range {
        KeyRange::All => 0,
        KeyRange::Exact(key) => key.0.len(),
        KeyRange::Range { start, end } => bound_len(start).or_else(|| bound_len(end)).unwrap_or(0),
    }
}

/// The first `len` components of `key`, or `None` when `len` already covers the
/// whole key so the caller can compare it directly without allocating.
fn prefix_of(key: &Key, len: usize) -> Option<Key> {
    (len < key.0.len()).then(|| Key(key.0[..len].to_vec()))
}

fn key_in_range(key: &Key, range: &KeyRange) -> bool {
    match range {
        KeyRange::All => true,
        KeyRange::Exact(exact) => key == exact,
        KeyRange::Range { start, end } => {
            let after_start = match start {
                Bound::Included(start) => key >= start,
                Bound::Excluded(start) => key > start,
                Bound::Unbounded => true,
            };
            let before_end = match end {
                Bound::Included(end) => key <= end,
                Bound::Excluded(end) => key < end,
                Bound::Unbounded => true,
            };
            after_start && before_end
        }
    }
}

/// Whether `key` is past the range's end bound, so a sorted scan can stop.
fn beyond_end(key: &Key, range: &KeyRange) -> bool {
    match range {
        KeyRange::All => false,
        KeyRange::Exact(exact) => key > exact,
        KeyRange::Range { end, .. } => match end {
            Bound::Included(end) => key > end,
            Bound::Excluded(end) => key >= end,
            Bound::Unbounded => false,
        },
    }
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use buffer::{MemoryBufferPool, PageStore};
    use common::{Key, KeyRange, Value};
    use wal::FileWalManager;

    use super::*;
    use crate::HeapPageStore;
    use crate::engine::RowLocation;

    const INDEX_FILE: FileId = 0x8000_0001;
    const SECONDARY_FILE: FileId = 0xC000_0001;

    struct Fixture {
        buffer: Arc<MemoryBufferPool>,
        wal: Arc<FileWalManager>,
        _dir: tempfile::TempDir,
    }

    impl Fixture {
        fn new(frames: usize) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn PageStore> =
                Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
            let buffer = Arc::new(MemoryBufferPool::new(frames, Box::new(AlwaysFlush), store));
            buffer.enable_stealing();
            let wal = Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
            Self {
                buffer,
                wal,
                _dir: dir,
            }
        }

        fn tree(&self) -> BTree<'_, RowLocation> {
            BTree::new(self.buffer.as_ref(), self.wal.as_ref(), INDEX_FILE)
        }

        fn secondary_tree(&self) -> BTree<'_, Key> {
            BTree::new(self.buffer.as_ref(), self.wal.as_ref(), SECONDARY_FILE)
        }
    }

    struct AlwaysFlush;
    impl common::FlushPolicy for AlwaysFlush {
        fn can_flush(&self, _info: &common::PageFlushInfo) -> bool {
            true
        }
    }

    fn key(value: i64) -> Key {
        Key(vec![Value::Integer(value)])
    }

    fn location(page_num: PageNum, slot_num: u16) -> RowLocation {
        RowLocation {
            file_id: 1,
            page_num,
            slot_num,
        }
    }

    #[test]
    fn insert_then_search_round_trips() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        assert!(tree.insert(1, &key(5), &location(0, 2)).unwrap());
        assert_eq!(tree.search(&key(5)).unwrap(), Some(location(0, 2)));
        assert_eq!(tree.search(&key(6)).unwrap(), None);
    }

    #[test]
    fn duplicate_insert_is_rejected() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        assert!(tree.insert(1, &key(1), &location(0, 0)).unwrap());
        assert!(!tree.insert(1, &key(1), &location(0, 9)).unwrap());
        assert_eq!(tree.search(&key(1)).unwrap(), Some(location(0, 0)));
    }

    #[test]
    fn update_replaces_location_in_place() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        tree.insert(1, &key(1), &location(0, 0)).unwrap();

        assert!(tree.update(1, &key(1), &location(3, 7)).unwrap());
        assert_eq!(tree.search(&key(1)).unwrap(), Some(location(3, 7)));
        assert!(!tree.update(1, &key(2), &location(0, 0)).unwrap());
    }

    #[test]
    fn remove_deletes_entry() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        tree.insert(1, &key(1), &location(0, 0)).unwrap();

        assert!(tree.remove(1, &key(1)).unwrap());
        assert_eq!(tree.search(&key(1)).unwrap(), None);
        assert!(!tree.remove(1, &key(1)).unwrap());
    }

    #[test]
    fn many_inserts_split_and_remain_searchable() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Enough keys to force multiple leaf splits and at least one root split.
        let n = 500i64;
        for value in 0..n {
            assert!(
                tree.insert(1, &key(value), &location(value as u32, 0))
                    .unwrap()
            );
        }
        for value in 0..n {
            assert_eq!(
                tree.search(&key(value)).unwrap(),
                Some(location(value as u32, 0)),
                "missing key {value}"
            );
        }
        assert_eq!(tree.search(&key(n)).unwrap(), None);
    }

    #[test]
    fn range_scan_returns_keys_in_order_across_leaves() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        for value in (0..200i64).rev() {
            tree.insert(1, &key(value), &location(value as u32, 0))
                .unwrap();
        }

        let all = tree.range(&KeyRange::All).unwrap();
        let keys: Vec<_> = all.iter().map(|(k, _)| k.clone()).collect();
        let expected: Vec<_> = (0..200i64).map(key).collect();
        assert_eq!(keys, expected);

        let bounded = tree
            .range(&KeyRange::Range {
                start: Bound::Included(key(10)),
                end: Bound::Excluded(key(13)),
            })
            .unwrap();
        let bounded_keys: Vec<_> = bounded.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(bounded_keys, vec![key(10), key(11), key(12)]);
    }

    #[test]
    fn delete_then_reinsert_after_splits() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        for value in 0..300i64 {
            tree.insert(1, &key(value), &location(value as u32, 0))
                .unwrap();
        }
        for value in (0..300i64).step_by(2) {
            assert!(tree.remove(1, &key(value)).unwrap());
        }
        for value in 0..300i64 {
            let expected = (value % 2 != 0).then(|| location(value as u32, 0));
            assert_eq!(tree.search(&key(value)).unwrap(), expected);
        }
        // A removed key can be reinserted.
        assert!(tree.insert(1, &key(0), &location(99, 1)).unwrap());
        assert_eq!(tree.search(&key(0)).unwrap(), Some(location(99, 1)));
    }

    #[test]
    fn large_variable_length_keys_split_by_bytes() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Each key fills most of a page, so two cannot share one node. A
        // count-balanced split would overflow a half; a byte-balanced split must
        // place each key on its own page.
        let text_key =
            |value: i64| Key(vec![Value::Text(format!("{value:04}{}", "x".repeat(2600)))]);
        for value in 0..6i64 {
            assert!(
                tree.insert(1, &text_key(value), &location(value as u32, 0))
                    .unwrap(),
                "insert of large key {value} failed"
            );
        }
        for value in 0..6i64 {
            assert_eq!(
                tree.search(&text_key(value)).unwrap(),
                Some(location(value as u32, 0))
            );
        }
        let ordered: Vec<_> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ordered, (0..6i64).map(text_key).collect::<Vec<_>>());
    }

    #[test]
    fn stores_primary_key_values_for_a_secondary_index() {
        let fixture = Fixture::new(64);
        let tree = fixture.secondary_tree();
        tree.create(1).unwrap();

        // Secondary-index shape: key = [indexed_value, pk], value = pk. The same
        // indexed value (10) appears twice, distinguished by the trailing pk.
        let entry = |indexed: i64, pk: i64| {
            (
                Key(vec![Value::Integer(indexed), Value::Integer(pk)]),
                Key(vec![Value::Integer(pk)]),
            )
        };
        for (indexed, pk) in [(20, 3), (10, 1), (10, 2)] {
            let (composite, primary) = entry(indexed, pk);
            assert!(tree.insert(1, &composite, &primary).unwrap());
        }

        let (composite, primary) = entry(10, 1);
        assert_eq!(tree.search(&composite).unwrap(), Some(primary));

        // Range order follows the composite key, so pks come back 1, 2, 3.
        let pks: Vec<_> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, pk)| pk)
            .collect();
        assert_eq!(
            pks,
            vec![
                Key(vec![Value::Integer(1)]),
                Key(vec![Value::Integer(2)]),
                Key(vec![Value::Integer(3)]),
            ]
        );
    }

    #[test]
    fn range_matches_indexed_prefix_ignoring_trailing_primary_key() {
        let fixture = Fixture::new(64);
        let tree = fixture.secondary_tree();
        tree.create(1).unwrap();

        for (indexed, pk) in [(10, 1), (10, 5), (20, 2), (30, 3)] {
            tree.insert(
                1,
                &Key(vec![Value::Integer(indexed), Value::Integer(pk)]),
                &Key(vec![Value::Integer(pk)]),
            )
            .unwrap();
        }
        let pks = |entries: Vec<(Key, Key)>| -> Vec<i64> {
            entries
                .into_iter()
                .map(|(_, pk)| match pk.0[0] {
                    Value::Integer(value) => value,
                    _ => unreachable!(),
                })
                .collect()
        };

        // Equality on the indexed value returns every row sharing it (both pks),
        // though the stored keys differ in their trailing pk.
        let eq = tree
            .range(&KeyRange::Exact(Key(vec![Value::Integer(10)])))
            .unwrap();
        assert_eq!(pks(eq), vec![1, 5]);

        // An inclusive upper bound on the indexed value still includes its rows,
        // which a naive full-key compare would wrongly drop (since [20, pk] > [20]).
        let inclusive = tree
            .range(&KeyRange::Range {
                start: Bound::Included(Key(vec![Value::Integer(20)])),
                end: Bound::Included(Key(vec![Value::Integer(20)])),
            })
            .unwrap();
        assert_eq!(pks(inclusive), vec![2]);

        // A half-open range over the indexed value.
        let bounded = tree
            .range(&KeyRange::Range {
                start: Bound::Included(Key(vec![Value::Integer(10)])),
                end: Bound::Excluded(Key(vec![Value::Integer(30)])),
            })
            .unwrap();
        assert_eq!(pks(bounded), vec![1, 5, 2]);
    }
}
