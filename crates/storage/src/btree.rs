//! On-disk, non-clustered index: a multi-entry B+-tree ordered by the composite
//! `(key, value)` living in its own file, separate from the table heap. Rows stay
//! in the heap; this tree replaces the in-memory primary-key directory.
//!
//! Duplicate user-keys are allowed; entries sharing a key are ordered and
//! disambiguated by their `value` (the heap `RowLocation`). The primary-key index
//! stores exactly one entry per key in this milestone (single version);
//! engine-level uniqueness is enforced by a presence probe before insert,
//! replacing the structural duplicate-key rejection the tree used to do (see
//! `engine.rs`). Secondary indexes are now uniform with the primary-key index:
//! they store the heap `RowLocation` directly (keyed by the indexed column(s)
//! alone, no embedded primary key), so duplicate indexed values coexist as
//! `(key, tid)` entries.
//!
//! Page 0 is the metapage (holds the root page number); other pages are leaf or
//! internal nodes (`index_page`). Leaves are singly linked left-to-right for
//! range scans. Insert splits nodes; delete removes the entry without merging
//! (accepted bloat). Every node mutation logs a `FullPageImage` and stamps the
//! page-LSN, so the tree is crash-safe through the same redo path as the heap.
//!
//! On-disk layout note: the node format is unchanged (slotted `[key_len][key]
//! [value]` entries). A leaf entry is `(encoded user key, value)` as before. An
//! internal separator's *key* field now holds the composite bytes
//! `encoded user key ++ value` of the boundary leaf entry (its *value* field is
//! still a child page number), so routing can disambiguate equal user-keys that
//! straddle a node split. The leading encoded key is self-delimiting
//! (`decode_key_prefix`), so the trailing value tiebreaker is recovered without a
//! length prefix and no format version bump is needed.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Bound;

use buffer::{BufferPool, PageWriteGuard};
use common::{DbError, FileId, Key, KeyRange, PageNum, QueryCancel, Result, SqlState};
use wal::{WalManager, WalRecord};

use crate::codec::{decode_key, decode_key_prefix, encode_key};
use crate::engine::{RowLocation, fpi_record_kind};
use crate::index_page;

const META_PAGE: PageNum = 0;
const LOCATION_LEN: usize = 10;
const CHILD_LEN: usize = 4;

type PageImage = [u8; buffer::PAGE_SIZE];

/// A value stored in a B-tree leaf. Every index (primary-key and secondary) stores
/// a fixed-width `RowLocation` (heap TID); the trait keeps the value encoding
/// pluggable. The tree itself treats values as opaque bytes; this trait is the only
/// place a value's on-page encoding is defined. Entries with equal user-keys are
/// ordered by the raw value bytes, so `encode` doubles as the value's sort key.
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
        let file_id = bytes
            .get(0..4)
            .ok_or_else(|| corrupt("index row-location file id is truncated"))?
            .try_into()
            .map_err(|_| corrupt("index row-location file id has the wrong width"))?;
        let page_num = bytes
            .get(4..8)
            .ok_or_else(|| corrupt("index row-location page number is truncated"))?
            .try_into()
            .map_err(|_| corrupt("index row-location page number has the wrong width"))?;
        let slot_num = bytes
            .get(8..10)
            .ok_or_else(|| corrupt("index row-location slot number is truncated"))?
            .try_into()
            .map_err(|_| corrupt("index row-location slot number has the wrong width"))?;
        Ok(RowLocation {
            file_id: u32::from_le_bytes(file_id),
            page_num: u32::from_le_bytes(page_num),
            slot_num: u16::from_le_bytes(slot_num),
        })
    }
}

/// A B+-tree over one index file, generic over its leaf value type `V`. Reads
/// need only the buffer pool; mutations also log redo through the WAL under the
/// statement's `txn_id`.
pub(crate) struct BTree<'a, V> {
    buffer: &'a dyn BufferPool,
    wal: &'a dyn WalManager,
    file_id: FileId,
    compression: &'a compress::CompressionRegistry,
    _value: PhantomData<fn() -> V>,
}

enum InsertOutcome {
    Inserted,
    Split {
        /// Composite separator bytes (`encoded key ++ value`) of the right half's
        /// first entry — enough to route equal user-keys across the split.
        sep_key: Vec<u8>,
        right_page: PageNum,
    },
}

struct PendingInsert<'a> {
    pos: u16,
    key_bytes: &'a [u8],
    value: &'a [u8],
    leaf: bool,
}

#[derive(Clone, Copy)]
enum WriteMode {
    Logged(u64),
    Unlogged,
}

impl WriteMode {
    fn txn_id(self) -> u64 {
        match self {
            Self::Logged(txn_id) => txn_id,
            Self::Unlogged => 0,
        }
    }

    fn wal_txn_id(self) -> Option<u64> {
        match self {
            Self::Logged(txn_id) => Some(txn_id),
            Self::Unlogged => None,
        }
    }

    fn is_unlogged(self) -> bool {
        matches!(self, Self::Unlogged)
    }
}

/// A search probe ordered against stored entries by `(key, value)`. The optional
/// value is the tiebreaker among equal user-keys: `None` is a lower bound (sorts
/// before every entry sharing the key), `Some(bytes)` targets one exact entry.
struct Probe<'a> {
    key: &'a Key,
    value: Option<&'a [u8]>,
}

impl<'a, V: IndexValue> BTree<'a, V> {
    pub(crate) fn new(
        buffer: &'a dyn BufferPool,
        wal: &'a dyn WalManager,
        file_id: FileId,
        compression: &'a compress::CompressionRegistry,
    ) -> Self {
        Self {
            buffer,
            wal,
            file_id,
            compression,
            _value: PhantomData,
        }
    }

    /// Create an empty index: a metapage (page 0) pointing at a fresh empty root
    /// leaf (page 1).
    pub(crate) fn create(&self, txn_id: u64) -> Result<()> {
        let meta = self.buffer.new_page(self.file_id, txn_id)?;
        let meta_num = meta.page_num();
        let root = self.buffer.new_page(self.file_id, txn_id)?;
        let root_num = root.page_num();

        let mut root_image = *root.data();
        index_page::init(&mut root_image, root_num, true);
        if let Err(err) = self.log_new_full_page(WriteMode::Logged(txn_id), root, root_image) {
            self.abandon_unpublished_new_page(meta)?;
            return Err(err);
        }

        let mut meta_image = *meta.data();
        index_page::meta_init(&mut meta_image, meta_num, root_num);
        self.log_new_full_page(WriteMode::Logged(txn_id), meta, meta_image)?;
        Ok(())
    }

    /// Reset during a derived rebuild and log the new root/metapage images.
    pub(crate) fn reset_to_empty(&self, txn_id: u64) -> Result<()> {
        self.reset_to_empty_with_mode(WriteMode::Logged(txn_id))
    }

    /// Reset during recovery's derived rebuild, where WAL replay already reached
    /// the final heap state and recovery must not append new WAL.
    pub(crate) fn reset_to_empty_unlogged(&self) -> Result<()> {
        self.reset_to_empty_with_mode(WriteMode::Unlogged)
    }

    fn reset_to_empty_with_mode(&self, mode: WriteMode) -> Result<()> {
        let mut root_image = [0u8; buffer::PAGE_SIZE];
        index_page::init(&mut root_image, 1, true);
        self.write_root_page(mode, 1, root_image)?;

        let mut meta_image = [0u8; buffer::PAGE_SIZE];
        index_page::meta_init(&mut meta_image, 0, 1);
        self.write_root_page(mode, 0, meta_image)?;
        Ok(())
    }

    /// The first value stored for `key`, or `None`. With duplicate keys allowed a
    /// key may have several values; this returns the lowest by value order, which
    /// is the single value for the (single-version) primary-key index.
    ///
    /// Visibility-unaware: with versioning (Milestone B4) a key may carry several
    /// versions' entries, so the engine locates rows via
    /// `locate_visible_version`/`scan_key` instead. `search` stays as part of the
    /// B-tree's stable single-value lookup contract (exercised by the B-tree unit
    /// tests) and remains available to callers that want exact `(key, value)`
    /// presence.
    #[allow(dead_code, reason = "single-value lookup; no engine caller after B4.9")]
    pub(crate) fn search(&self, key: &Key) -> Result<Option<V>> {
        let probe = Probe { key, value: None };
        let mut page_num = self.descend_to_leaf(&probe)?;
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            let count = index_page::entry_count(data);
            let start = self.lower_bound(data, true, &probe)?;
            if start < count {
                let (entry_key, value) = leaf_key_value(data, start)?;
                if &entry_key == key {
                    return Ok(Some(V::decode(value)?));
                }
                return Ok(None);
            }
            // The key could be the first entry of the next leaf if it lands on a
            // leaf boundary; follow the right-sibling link to check.
            let next = index_page::link(data);
            if next == 0 {
                return Ok(None);
            }
            page_num = next;
        }
    }

    /// Every value whose user-key equals `key`, in `(key, value)` order. Walks the
    /// leaf chain from the key's lower bound until a larger key is seen.
    pub(crate) fn scan_key(&self, key: &Key) -> Result<Vec<V>> {
        let probe = Probe { key, value: None };
        let mut page_num = self.descend_to_leaf(&probe)?;
        let mut out = Vec::new();
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            let count = index_page::entry_count(data);
            let mut pos = self.lower_bound(data, true, &probe)?;
            while pos < count {
                let (entry_key, value) = leaf_key_value(data, pos)?;
                if &entry_key != key {
                    return Ok(out);
                }
                out.push(V::decode(value)?);
                pos += 1;
            }
            let next = index_page::link(data);
            if next == 0 {
                return Ok(out);
            }
            page_num = next;
        }
    }

    /// Insert the entry `(key, value)`. Duplicate user-keys are allowed; the entry
    /// is placed in `(key, value)` order. An exact `(key, value)` duplicate is also
    /// inserted (the engine prevents that for the primary-key index).
    pub(crate) fn insert(&self, txn_id: u64, key: &Key, value: &V) -> Result<()> {
        self.insert_with_mode(WriteMode::Logged(txn_id), key, value)
    }

    /// Insert during a derived rebuild whose logical WAL record is already durable.
    /// This updates pages without appending physical B-tree WAL.
    pub(crate) fn insert_unlogged(&self, key: &Key, value: &V) -> Result<()> {
        self.insert_with_mode(WriteMode::Unlogged, key, value)
    }

    fn insert_with_mode(&self, mode: WriteMode, key: &Key, value: &V) -> Result<()> {
        let key_bytes = encode_key(key)?;
        let value = value.encode()?;
        validate_index_entry_fits(key_bytes.len(), value.len())?;
        let probe = Probe {
            key,
            value: Some(&value),
        };
        let root = self.root()?;
        if let InsertOutcome::Split {
            sep_key,
            right_page,
        } = self.insert_rec(mode, root, &probe, &key_bytes, &value)?
        {
            // The root split: grow the tree by one level with a new internal
            // root whose leftmost child is the old root.
            let new_root = self.buffer.new_page(self.file_id, mode.txn_id())?;
            let new_root_num = new_root.page_num();
            let mut image = *new_root.data();
            index_page::init(&mut image, new_root_num, false);
            index_page::set_link(&mut image, root);
            if let Err(err) =
                index_page::insert_entry(&mut image, 0, &sep_key, &encode_child(right_page))
            {
                self.abandon_unpublished_new_page(new_root)?;
                return Err(err);
            }
            self.log_new_full_page(mode, new_root, image)?;
            self.set_root(mode, new_root_num)?;
        }
        Ok(())
    }

    /// Remove the single `(key, value)` entry. Returns `false` if no entry with
    /// that exact key *and* value exists. Other entries sharing the key are left
    /// intact. Underfull nodes are not merged (accepted bloat).
    ///
    /// MVCC DML never removes an index entry (DELETE/UPDATE retain every version's
    /// entry; `docs/specs/mvcc.md` §3.2 invariant 3) — entry removal is VACUUM's job
    /// (Milestone F), which is this method's next caller. It stays as part of the
    /// B-tree's stable `(key, value)` removal contract (exercised by the B-tree unit
    /// tests) until then.
    #[allow(dead_code, reason = "entry removal is VACUUM's job (Milestone F)")]
    pub(crate) fn remove(&self, txn_id: u64, key: &Key, value: &V) -> Result<bool> {
        let value = value.encode()?;
        let probe = Probe {
            key,
            value: Some(&value),
        };
        let mut page_num = self.descend_to_leaf(&probe)?;
        loop {
            let mut guard = self.buffer.write_page(self.file_id, page_num, txn_id)?;
            let data = guard.data();
            let count = index_page::entry_count(data);
            let pos = self.lower_bound(data, true, &probe)?;
            if pos < count {
                let (entry_key, entry_value) = leaf_key_value(data, pos)?;
                if &entry_key == key && entry_value == value.as_slice() {
                    let mut image = *guard.data();
                    index_page::remove_entry(&mut image, pos)?;
                    self.log_full_page(WriteMode::Logged(txn_id), &mut guard, image)?;
                    return Ok(true);
                }
                // The lower bound landed before the target but it is not a match,
                // so the entry is absent (entries are sorted by `(key, value)`).
                return Ok(false);
            }
            // The exact entry could be the first on the next leaf at a boundary.
            let next = index_page::link(data);
            if next == 0 {
                return Ok(false);
            }
            drop(guard);
            page_num = next;
        }
    }

    /// The leftmost leaf of the tree: descend the left spine (following each
    /// internal node's leftmost-child `link`) until a leaf is reached. The leaf
    /// chain starts here.
    fn first_leaf(&self) -> Result<PageNum> {
        let mut page_num = self.root()?;
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            if index_page::is_leaf(data) {
                return Ok(page_num);
            }
            page_num = index_page::link(data);
        }
    }

    /// Collect `(key, value)` for every entry within `range`, in `(key, value)`
    /// order. A user-key may now appear multiple times (one per value); all are
    /// returned.
    ///
    /// Comparison uses only the leading components of each key that the range's
    /// bounds constrain (their length). For the primary-key index the bounds are
    /// full keys, so this is an exact-key range. For a secondary index each stored
    /// key is just `[indexed..]` (no embedded primary key) and equal indexed values
    /// are disambiguated by the leaf `value` (heap TID), so an equality bound
    /// matches every row sharing the indexed value. Entries are returned with their
    /// full key.
    #[cfg(test)]
    pub(crate) fn range(&self, range: &KeyRange) -> Result<Vec<(Key, V)>> {
        self.range_cancelable(range, None)
    }

    pub(crate) fn range_cancelable(
        &self,
        range: &KeyRange,
        cancel: Option<&QueryCancel>,
    ) -> Result<Vec<(Key, V)>> {
        let mut out = Vec::new();
        self.range_for_each_cancelable(range, cancel, |key, value| {
            out.push((key, value));
            Ok(())
        })?;
        Ok(out)
    }

    pub(crate) fn range_for_each_cancelable<F>(
        &self,
        range: &KeyRange,
        cancel: Option<&QueryCancel>,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(Key, V) -> Result<()>,
    {
        let prefix_len = comparison_prefix_len(range);
        let mut page_num = self.start_leaf(range)?;
        loop {
            if let Some(cancel) = cancel {
                cancel.check()?;
            }
            let (entries, next, done) = {
                let guard = self.buffer.read_page(self.file_id, page_num)?;
                let data = guard.data();
                let mut entries = Vec::new();
                let mut done = false;
                for pos in 0..index_page::entry_count(data) {
                    let (full, value) = leaf_key_value(data, pos)?;
                    let probe = prefix_of(&full, prefix_len);
                    let compared = probe.as_ref().unwrap_or(&full);
                    if beyond_end(compared, range) {
                        done = true;
                        break;
                    }
                    if key_in_range(compared, range) {
                        entries.push((full, V::decode(value)?));
                    }
                }
                (entries, index_page::link(data), done)
            };

            for (key, value) in entries {
                visitor(key, value)?;
            }
            if done || next == 0 {
                return Ok(());
            }
            page_num = next;
        }
    }

    fn insert_rec(
        &self,
        mode: WriteMode,
        page_num: PageNum,
        probe: &Probe<'_>,
        key_bytes: &[u8],
        value: &[u8],
    ) -> Result<InsertOutcome> {
        if self.node_is_leaf(page_num)? {
            let mut page_num = page_num;
            loop {
                let mut guard = self
                    .buffer
                    .write_page(self.file_id, page_num, mode.txn_id())?;
                let mut pos = self.lower_bound(guard.data(), true, probe)?;
                if pos == index_page::entry_count(guard.data()) {
                    let right = index_page::link(guard.data());
                    if right != 0 {
                        drop(guard);
                        if let Some(next) = self.right_leaf_for_probe(right, probe)? {
                            page_num = next;
                            continue;
                        }
                        guard = self
                            .buffer
                            .write_page(self.file_id, page_num, mode.txn_id())?;
                        pos = self.lower_bound(guard.data(), true, probe)?;
                    }
                }

                if index_page::has_space(guard.data(), key_bytes.len(), value.len()) {
                    let mut image = *guard.data();
                    index_page::insert_entry(&mut image, pos, key_bytes, value)?;
                    self.log_full_page(mode, &mut guard, image)?;
                    return Ok(InsertOutcome::Inserted);
                }
                return self.split_node(
                    mode,
                    &mut guard,
                    PendingInsert {
                        pos,
                        key_bytes,
                        value,
                        leaf: true,
                    },
                );
            }
        }

        let child = {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            self.child_for(guard.data(), probe)?
        };
        match self.insert_rec(mode, child, probe, key_bytes, value)? {
            InsertOutcome::Split {
                sep_key,
                right_page,
            } => {
                let mut guard = self
                    .buffer
                    .write_page(self.file_id, page_num, mode.txn_id())?;
                // Route the separator by its own `(key, value)`: split the
                // composite into its user key and the value tiebreaker.
                let (sep_decoded, consumed) = decode_key_prefix(&sep_key)?;
                let sep_probe = Probe {
                    key: &sep_decoded,
                    value: Some(&sep_key[consumed..]),
                };
                let pos = self.lower_bound(guard.data(), false, &sep_probe)?;
                let child_bytes = encode_child(right_page);
                if index_page::has_space(guard.data(), sep_key.len(), child_bytes.len()) {
                    let mut image = *guard.data();
                    index_page::insert_entry(&mut image, pos, &sep_key, &child_bytes)?;
                    self.log_full_page(mode, &mut guard, image)?;
                    Ok(InsertOutcome::Inserted)
                } else {
                    self.split_node(
                        mode,
                        &mut guard,
                        PendingInsert {
                            pos,
                            key_bytes: &sep_key,
                            value: &child_bytes,
                            leaf: false,
                        },
                    )
                }
            }
            outcome => Ok(outcome),
        }
    }

    /// Split a full node, placing the new entry at `pos`, and return the
    /// separator pushed to the parent plus the new right page. For a leaf the
    /// separator is the composite `(key ++ value)` of the right half's first
    /// entry; for an internal node the middle entry's composite key moves up and
    /// its child becomes the right node's leftmost.
    fn split_node(
        &self,
        mode: WriteMode,
        guard: &mut PageWriteGuard,
        insert: PendingInsert<'_>,
    ) -> Result<InsertOutcome> {
        let entries =
            entries_with_insertion(guard.data(), insert.pos, insert.key_bytes, insert.value);
        let leftmost = index_page::link(guard.data());
        let old_right_link = leftmost; // for a leaf, link is the right sibling

        let page_num = guard.page_num();
        let mid = split_point(&entries);

        if insert.leaf {
            let right = self.buffer.new_page(self.file_id, mode.txn_id())?;
            let right_num = right.page_num();
            let mut right_image = *right.data();
            index_page::init(&mut right_image, right_num, true);
            if let Err(err) = append_entries(&mut right_image, &entries[mid..]) {
                self.abandon_unpublished_new_page(right)?;
                return Err(err);
            }
            index_page::set_link(&mut right_image, old_right_link);
            self.log_new_full_page(mode, right, right_image)?;

            let mut left_image = *guard.data();
            index_page::init(&mut left_image, page_num, true);
            append_entries(&mut left_image, &entries[..mid])?;
            index_page::set_link(&mut left_image, right_num);
            self.log_full_page(mode, guard, left_image)?;

            // The separator is the composite `(key ++ value)` of the right half's
            // first leaf entry, so the parent can route equal user-keys that
            // straddle this boundary by their value.
            let sep_key = leaf_separator(&entries[mid]);
            Ok(InsertOutcome::Split {
                sep_key,
                right_page: right_num,
            })
        } else {
            // The middle internal entry's composite key is already
            // `(key ++ value)`; push it up verbatim and hand its child to the
            // right node as its new leftmost child.
            let mid = internal_split_point(&entries, mid).ok_or_else(|| {
                corrupt("internal index split cannot fit a prefix-safe separator")
            })?;
            let push_key = entries[mid].0.clone();
            let right_leftmost = decode_child(&entries[mid].1)?;

            let right = self.buffer.new_page(self.file_id, mode.txn_id())?;
            let right_num = right.page_num();
            let mut right_image = *right.data();
            index_page::init(&mut right_image, right_num, false);
            index_page::set_link(&mut right_image, right_leftmost);
            if let Err(err) = append_entries(&mut right_image, &entries[mid + 1..]) {
                self.abandon_unpublished_new_page(right)?;
                return Err(err);
            }
            self.log_new_full_page(mode, right, right_image)?;

            let mut left_image = *guard.data();
            index_page::init(&mut left_image, page_num, false);
            index_page::set_link(&mut left_image, leftmost);
            append_entries(&mut left_image, &entries[..mid])?;
            index_page::insert_entry(
                &mut left_image,
                mid as u16,
                &push_key,
                &encode_child(right_num),
            )?;
            self.log_full_page(mode, guard, left_image)?;

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

    fn set_root(&self, mode: WriteMode, root: PageNum) -> Result<()> {
        let mut guard = self
            .buffer
            .write_page(self.file_id, META_PAGE, mode.txn_id())?;
        let mut image = *guard.data();
        index_page::meta_set_root(&mut image, root);
        self.log_full_page(mode, &mut guard, image)
    }

    fn node_is_leaf(&self, page_num: PageNum) -> Result<bool> {
        let guard = self.buffer.read_page(self.file_id, page_num)?;
        Ok(index_page::is_leaf(guard.data()))
    }

    /// Descend from the root to the leaf that should hold `probe`.
    fn descend_to_leaf(&self, probe: &Probe<'_>) -> Result<PageNum> {
        let mut page_num = self.root()?;
        loop {
            let guard = self.buffer.read_page(self.file_id, page_num)?;
            let data = guard.data();
            if index_page::is_leaf(data) {
                return Ok(page_num);
            }
            page_num = self.child_for(data, probe)?;
        }
    }

    /// The index of the first entry that is `>= probe` under `(key, value)`
    /// ordering (a classic lower bound). `is_leaf` selects how each entry's
    /// comparison key is parsed: a leaf entry is `(encoded key, value)`; an
    /// internal entry is a composite `key` field (`encoded key ++ value`) whose
    /// value field is the child pointer and is ignored.
    fn lower_bound(
        &self,
        data: &[u8; buffer::PAGE_SIZE],
        is_leaf: bool,
        probe: &Probe<'_>,
    ) -> Result<u16> {
        self.bound(data, is_leaf, probe, false)
    }

    /// The index of the first entry strictly `> probe` (an upper bound). Used to
    /// route an internal node: a separator equal to the probe belongs to the
    /// right subtree (it is the right half's first key), so routing descends to
    /// the left of the first *strictly greater* separator.
    fn upper_bound(
        &self,
        data: &[u8; buffer::PAGE_SIZE],
        is_leaf: bool,
        probe: &Probe<'_>,
    ) -> Result<u16> {
        self.bound(data, is_leaf, probe, true)
    }

    /// Binary search for an insertion point. With `strict = false` this is the
    /// lower bound (first entry `>= probe`); with `strict = true` it is the upper
    /// bound (first entry `> probe`).
    fn bound(
        &self,
        data: &[u8; buffer::PAGE_SIZE],
        is_leaf: bool,
        probe: &Probe<'_>,
        strict: bool,
    ) -> Result<u16> {
        let count = index_page::entry_count(data);
        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let go_right = match self.entry_cmp(data, is_leaf, mid, probe)? {
                Ordering::Less => true,
                Ordering::Equal => strict,
                Ordering::Greater => false,
            };
            if go_right {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Compare stored entry `index` against `probe` by `(key, value)`.
    fn entry_cmp(
        &self,
        data: &[u8; buffer::PAGE_SIZE],
        is_leaf: bool,
        index: u16,
        probe: &Probe<'_>,
    ) -> Result<Ordering> {
        let (entry_key, entry_value) = if is_leaf {
            leaf_key_value(data, index)?
        } else {
            internal_key_value(data, index)?
        };
        Ok(match entry_key.cmp(probe.key) {
            Ordering::Equal => match probe.value {
                // A `None` probe value is a lower bound: it sorts before any
                // stored value, so an equal-key entry is `Greater` than the probe.
                None => Ordering::Greater,
                Some(value) => entry_value.cmp(value),
            },
            other => other,
        })
    }

    /// The child subtree of an internal node that may contain `probe`. A
    /// separator is the right child's *first* key, so child `i` holds entries
    /// `< separator[i]` and child `i+1` holds entries `>= separator[i]`. The
    /// correct child is therefore the one immediately left of the first separator
    /// *strictly greater* than the probe (`upper_bound`); a separator equal to the
    /// probe routes right into its own child.
    fn child_for(&self, data: &[u8; buffer::PAGE_SIZE], probe: &Probe<'_>) -> Result<PageNum> {
        let pos = self.upper_bound(data, false, probe)?;
        if pos == 0 {
            Ok(index_page::link(data)) // leftmost child
        } else {
            decode_child(index_page::entry_value(data, pos - 1))
        }
    }

    fn right_leaf_for_probe(
        &self,
        mut next: PageNum,
        probe: &Probe<'_>,
    ) -> Result<Option<PageNum>> {
        while next != 0 {
            let guard = self.buffer.read_page(self.file_id, next)?;
            let next_data = guard.data();
            if index_page::entry_count(next_data) == 0 {
                next = index_page::link(next_data);
                continue;
            }

            return match self.entry_cmp(next_data, true, 0, probe)? {
                Ordering::Less | Ordering::Equal => Ok(Some(next)),
                Ordering::Greater => Ok(None),
            };
        }
        Ok(None)
    }

    fn start_leaf(&self, range: &KeyRange) -> Result<PageNum> {
        match range_start_key(range) {
            Some(key) => self.descend_to_leaf(&Probe {
                key: &key,
                value: None,
            }),
            None => {
                let mut page_num = self.root()?;
                loop {
                    let guard = self.buffer.read_page(self.file_id, page_num)?;
                    let data = guard.data();
                    if index_page::is_leaf(data) {
                        return Ok(page_num);
                    }
                    page_num = index_page::link(data);
                }
            }
        }
    }

    fn log_full_page(
        &self,
        mode: WriteMode,
        guard: &mut PageWriteGuard,
        mut image: PageImage,
    ) -> Result<()> {
        let Some(txn_id) = mode.wal_txn_id() else {
            *guard.data_mut() = image;
            return Ok(());
        };
        let lsn = self.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: fpi_record_kind(self.compression, self.file_id, guard.page_num(), &image),
        })?;
        crate::page::set_page_lsn(&mut image, lsn);
        *guard.data_mut() = image;
        Ok(())
    }

    fn log_new_full_page(
        &self,
        mode: WriteMode,
        mut guard: PageWriteGuard,
        image: PageImage,
    ) -> Result<()> {
        match self.log_full_page(mode, &mut guard, image) {
            Ok(()) => {
                // The FullPageImage now durably references this freshly allocated
                // page: it can no longer be abandoned, only reclaimed by VACUUM.
                guard.publish();
                Ok(())
            }
            Err(err) => {
                self.abandon_unpublished_new_page(guard)?;
                Err(err)
            }
        }
    }

    fn abandon_unpublished_new_page(&self, guard: PageWriteGuard) -> Result<()> {
        self.buffer.abandon_unpublished_new_page(guard)
    }

    fn write_root_page(&self, mode: WriteMode, page_num: PageNum, image: PageImage) -> Result<()> {
        let mut guard = if mode.is_unlogged() {
            self.buffer.fetch_for_redo(self.file_id, page_num)?
        } else {
            self.buffer
                .write_page(self.file_id, page_num, mode.txn_id())?
        };
        self.log_full_page(mode, &mut guard, image)
    }
}

impl<'a> BTree<'a, RowLocation> {
    /// Remove every leaf entry whose stored value (the heap `RowLocation`/TID) is in
    /// `dead`, returning how many were removed. This is index VACUUM's primitive
    /// (`docs/specs/mvcc.md` §9, Milestone F3a): after `vacuum_heap` prunes a dead
    /// tuple the heap key bytes are gone, so the dangling entry cannot be recomputed
    /// and removed by key — it is matched by value-set (dead-TID) membership instead.
    /// The pass walks the leaf chain once (left to right via the right-sibling
    /// `link`s), and for each leaf removes the matching entries under that leaf's
    /// frame write latch, logging a single `FullPageImage` of the leaf only when it
    /// changed. Entries with a value not in `dead` (live versions) are left intact.
    ///
    /// **B-link safety vs concurrent lock-free scanners.** A reader traverses leaves
    /// under a short-lived per-leaf read latch and follows the right-sibling `link` to
    /// the next; it takes no structural latch. This removal is safe against such a
    /// reader because:
    /// - It never merges or frees a leaf and never rewrites a right-sibling `link`, so
    ///   the leaf chain a reader is walking is structurally unchanged. An emptied leaf
    ///   stays in place (accepted bloat, mirroring the heap's leave-pages-in-place
    ///   stance); a reader landing on it finds no matching entries and follows `link`
    ///   as before.
    /// - Within one leaf the entry shift runs under that leaf's *write* latch, which is
    ///   mutually exclusive with a reader's *read* latch on the SAME leaf, so a reader
    ///   sees the leaf either fully before or fully after the shift, never torn.
    /// - A reader that already passed a leaf, or sits between two leaves, is
    ///   unaffected: right-links are never rewritten, so its traversal still reaches
    ///   every later leaf. A removed entry was a *dead* TID, never a live one, so no
    ///   live entry is ever shifted out of a concurrent reader's path — a scanner
    ///   never misses or duplicates a live entry.
    ///
    /// The whole pass runs under the index's per-index structural latch (held by the
    /// engine caller), so it never races another structural writer on this index.
    #[cfg(test)]
    pub(crate) fn remove_values_in(
        &self,
        txn_id: u64,
        dead: &std::collections::HashSet<RowLocation>,
    ) -> Result<usize> {
        self.remove_values_in_cancelable(txn_id, dead, None)
    }

    pub(crate) fn remove_values_in_cancelable(
        &self,
        txn_id: u64,
        dead: &std::collections::HashSet<RowLocation>,
        cancel: Option<&QueryCancel>,
    ) -> Result<usize> {
        if dead.is_empty() {
            return Ok(0);
        }
        let mut page_num = self.first_leaf()?;
        let mut removed = 0usize;
        loop {
            if let Some(cancel) = cancel {
                cancel.check()?;
            }
            let mut guard = self.buffer.write_page(self.file_id, page_num, txn_id)?;
            let mut image = *guard.data();
            // Walk entries from the end so each `remove_entry` shift never disturbs the
            // index of an entry still to be examined.
            let mut changed = false;
            let mut pos = index_page::entry_count(&image);
            while pos > 0 {
                pos -= 1;
                let value = RowLocation::decode(index_page::entry_value(&image, pos))?;
                if dead.contains(&value) {
                    index_page::remove_entry(&mut image, pos)?;
                    removed += 1;
                    changed = true;
                }
            }
            let next = index_page::link(&image);
            if changed {
                self.log_full_page(WriteMode::Logged(txn_id), &mut guard, image)?;
            }
            drop(guard);
            if next == 0 {
                return Ok(removed);
            }
            page_num = next;
        }
    }
}

/// The `(decoded key, value bytes)` of a leaf entry, borrowing the value bytes
/// from the page.
fn leaf_key_value(data: &[u8; buffer::PAGE_SIZE], index: u16) -> Result<(Key, &[u8])> {
    let key = decode_key(index_page::entry_key(data, index))?;
    Ok((key, index_page::entry_value(data, index)))
}

/// The `(decoded key, value tiebreaker bytes)` of an internal separator entry,
/// whose key field is the composite `encoded key ++ value`. The trailing value
/// bytes (which may be empty for an all-key separator) are the routing
/// tiebreaker; the entry's value field is the child pointer and is read
/// separately.
fn internal_key_value(data: &[u8; buffer::PAGE_SIZE], index: u16) -> Result<(Key, &[u8])> {
    let composite = index_page::entry_key(data, index);
    let (key, consumed) = decode_key_prefix(composite)?;
    Ok((key, &composite[consumed..]))
}

/// The composite separator bytes for a leaf entry: its `[key_len][key][value]`
/// body has the encoded key followed by the value, which is exactly the
/// `encoded key ++ value` form an internal separator stores in its key field.
fn leaf_separator(entry: &(Vec<u8>, Vec<u8>)) -> Vec<u8> {
    let (key_bytes, value_bytes) = entry;
    let mut sep = Vec::with_capacity(key_bytes.len() + value_bytes.len());
    sep.extend_from_slice(key_bytes);
    sep.extend_from_slice(value_bytes);
    sep
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

pub(crate) fn validate_index_entry_fits(key_len: usize, value_len: usize) -> Result<()> {
    let separator_len = key_len.checked_add(value_len).ok_or_else(|| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "index entry is too large for a b-tree page",
        )
    })?;
    if !index_page::entry_fits_empty_node(key_len, value_len)
        || !index_page::entry_fits_empty_node(separator_len, CHILD_LEN)
    {
        return Err(DbError::storage(
            SqlState::ProgramLimitExceeded,
            "index entry is too large for a b-tree page",
        ));
    }
    Ok(())
}

fn entries_fit_as_internal_node(entries: &[(Vec<u8>, Vec<u8>)]) -> bool {
    let mut data = [0; buffer::PAGE_SIZE];
    index_page::init(&mut data, 1, false);
    append_entries(&mut data, entries).is_ok()
}

fn internal_split_point(entries: &[(Vec<u8>, Vec<u8>)], preferred: usize) -> Option<usize> {
    if entries.is_empty() {
        return None;
    }

    let preferred = preferred.min(entries.len() - 1);
    for distance in 0..entries.len() {
        if let Some(left) = preferred.checked_sub(distance)
            && entries_fit_as_internal_node(&entries[..=left])
            && entries_fit_as_internal_node(&entries[left + 1..])
        {
            return Some(left);
        }

        let right = preferred + distance;
        if distance != 0
            && right < entries.len()
            && entries_fit_as_internal_node(&entries[..=right])
            && entries_fit_as_internal_node(&entries[right + 1..])
        {
            return Some(right);
        }
    }

    None
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

pub(crate) fn validate_index_key_fits(key: &Key) -> Result<()> {
    let key_bytes = encode_key(key)?;
    validate_index_entry_fits(key_bytes.len(), LOCATION_LEN)
}

fn encode_child(page: PageNum) -> [u8; CHILD_LEN] {
    page.to_le_bytes()
}

fn decode_child(bytes: &[u8]) -> Result<PageNum> {
    if bytes.len() != CHILD_LEN {
        return Err(corrupt("index internal value is not a child pointer"));
    }
    let bytes = bytes
        .try_into()
        .map_err(|_| corrupt("index child pointer has the wrong width"))?;
    Ok(u32::from_le_bytes(bytes))
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
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

    use buffer::{BufferPool, MemoryBufferPool, PageStore};
    use common::{DbError, Key, KeyRange, Lsn, SqlState, TxnId, TxnStatus, TxnStatusView, Value};
    use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

    use super::*;
    use crate::HeapPageStore;
    use crate::engine::RowLocation;

    const INDEX_FILE: FileId = 0x8000_0001;
    const SECONDARY_FILE: FileId = 0xC000_0001;
    const NO_FAIL_PAGE: PageNum = u32::MAX;

    struct Fixture {
        buffer: Arc<MemoryBufferPool>,
        wal: Arc<FileWalManager>,
        compression: compress::CompressionRegistry,
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
                compression: compress::CompressionRegistry::new(),
                _dir: dir,
            }
        }

        fn tree(&self) -> BTree<'_, RowLocation> {
            BTree::new(
                self.buffer.as_ref(),
                self.wal.as_ref(),
                INDEX_FILE,
                &self.compression,
            )
        }

        fn secondary_tree(&self) -> BTree<'_, RowLocation> {
            BTree::new(
                self.buffer.as_ref(),
                self.wal.as_ref(),
                SECONDARY_FILE,
                &self.compression,
            )
        }
    }

    struct AlwaysFlush;
    impl common::FlushPolicy for AlwaysFlush {
        fn can_flush(&self, _info: &common::PageFlushInfo) -> bool {
            true
        }
    }

    struct FailingWal {
        next_lsn: AtomicU64,
        fail_at: AtomicU64,
        fail_page: AtomicU32,
    }

    impl Default for FailingWal {
        fn default() -> Self {
            Self {
                next_lsn: AtomicU64::new(1),
                fail_at: AtomicU64::new(0),
                fail_page: AtomicU32::new(NO_FAIL_PAGE),
            }
        }
    }

    impl FailingWal {
        fn fail_next_append(&self) {
            self.fail_at
                .store(self.next_lsn.load(Ordering::SeqCst), Ordering::SeqCst);
        }

        fn fail_next_full_page_for_page(&self, page_num: PageNum) {
            self.fail_page.store(page_num, Ordering::SeqCst);
        }
    }

    impl WalManager for FailingWal {
        fn append(&self, record: WalRecord) -> common::Result<Lsn> {
            let next = self.next_lsn.load(Ordering::SeqCst);
            if self.fail_at.load(Ordering::SeqCst) == next {
                self.fail_at.store(0, Ordering::SeqCst);
                return Err(DbError::io("injected WAL append failure"));
            }
            // Both the raw and (now unconditionally attempted) compressed FPI
            // variants carry a `page_num`; match either so failure injection
            // still targets the intended page regardless of which one the
            // registry produced for it.
            let fpi_page_num = match record.kind {
                WalRecordKind::FullPageImage { page_num, .. }
                | WalRecordKind::FullPageImageCompressed { page_num, .. } => Some(page_num),
                _ => None,
            };
            if let Some(page_num) = fpi_page_num
                && self.fail_page.load(Ordering::SeqCst) == page_num
            {
                self.fail_page.store(NO_FAIL_PAGE, Ordering::SeqCst);
                return Err(DbError::io("injected WAL append failure"));
            }
            Ok(self.next_lsn.fetch_add(1, Ordering::SeqCst))
        }

        fn flush(&self) -> common::Result<Lsn> {
            Ok(self.next_lsn.load(Ordering::SeqCst).saturating_sub(1))
        }

        fn replay_from(
            &self,
            _lsn: Lsn,
        ) -> common::Result<Box<dyn Iterator<Item = common::Result<WalRecord>>>> {
            Ok(Box::new(std::iter::empty()))
        }

        fn truncate_before(&self, _lsn: Lsn) -> common::Result<()> {
            Ok(())
        }

        fn flushed_lsn(&self) -> Lsn {
            self.next_lsn.load(Ordering::SeqCst).saturating_sub(1)
        }

        fn bytes_after(&self, _lsn: Lsn) -> common::Result<u64> {
            Ok(0)
        }

        fn persist_clog(&self, _clog_lsn: Lsn) -> common::Result<()> {
            Ok(())
        }

        fn set_vacuum_floor(&self, _boundary: TxnId) -> common::Result<()> {
            Ok(())
        }

        fn establish_recovery_committed_floor(
            &self,
            _allocation_boundary: u64,
        ) -> common::Result<()> {
            Ok(())
        }

        fn resolve_in_flight_as_aborted(
            &self,
            _writer_xids: &std::collections::HashSet<u64>,
        ) -> common::Result<()> {
            Ok(())
        }
    }

    impl TxnStatusView for FailingWal {
        fn status(&self, _txn_id: TxnId) -> TxnStatus {
            TxnStatus::Committed
        }
    }

    fn key(value: i64) -> Key {
        Key(vec![Value::Integer(value)])
    }

    fn fat_key(value: i64) -> Key {
        Key(vec![Value::Text(format!("{value:04}{}", "x".repeat(2600)))])
    }

    fn location(page_num: PageNum, slot_num: u16) -> RowLocation {
        RowLocation {
            file_id: 1,
            page_num,
            slot_num,
        }
    }

    fn root_shape(buffer: &dyn BufferPool, file_id: FileId) -> (PageNum, bool, u16) {
        let root = index_page::meta_root(buffer.read_page(file_id, META_PAGE).unwrap().data());
        let guard = buffer.read_page(file_id, root).unwrap();
        (
            root,
            index_page::is_leaf(guard.data()),
            index_page::entry_count(guard.data()),
        )
    }

    fn next_insert_would_split_root_leaf(
        buffer: &dyn BufferPool,
        file_id: FileId,
        key: &Key,
        value: &RowLocation,
    ) -> bool {
        let (root, is_leaf, _) = root_shape(buffer, file_id);
        assert!(is_leaf, "expected root to still be a leaf");
        let guard = buffer.read_page(file_id, root).unwrap();
        let key_bytes = encode_key(key).unwrap();
        let value_bytes = value.encode().unwrap();
        !index_page::has_space(guard.data(), key_bytes.len(), value_bytes.len())
    }

    #[test]
    fn insert_then_search_round_trips() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        tree.insert(1, &key(5), &location(0, 2)).unwrap();
        assert_eq!(tree.search(&key(5)).unwrap(), Some(location(0, 2)));
        assert_eq!(tree.search(&key(6)).unwrap(), None);
    }

    #[test]
    fn insert_rejects_entry_too_large_for_btree_page() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        let oversized = Key(vec![Value::Text("x".repeat(buffer::PAGE_SIZE))]);

        let err = tree.insert(1, &oversized, &location(0, 2)).unwrap_err();

        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
        assert!(err.message.contains("index entry"));
        let (_root, _is_leaf, count) = root_shape(fixture.buffer.as_ref(), INDEX_FILE);
        assert_eq!(count, 0);
    }

    #[test]
    fn duplicate_keys_with_different_values_scan_in_value_order() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Same user-key, three distinct values; insert out of value order.
        tree.insert(1, &key(7), &location(0, 5)).unwrap();
        tree.insert(1, &key(7), &location(0, 1)).unwrap();
        tree.insert(1, &key(7), &location(0, 9)).unwrap();
        // An unrelated neighbor key to bound the scan on both sides.
        tree.insert(1, &key(6), &location(0, 0)).unwrap();
        tree.insert(1, &key(8), &location(0, 0)).unwrap();

        // `scan_key` returns all three values for key 7 in `(key, value)` order.
        let values = tree.scan_key(&key(7)).unwrap();
        assert_eq!(values, vec![location(0, 1), location(0, 5), location(0, 9)]);
        // `search` returns the lowest value for the key.
        assert_eq!(tree.search(&key(7)).unwrap(), Some(location(0, 1)));
    }

    #[test]
    fn remove_deletes_only_the_named_entry() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        for slot in [1u16, 5, 9] {
            tree.insert(1, &key(7), &location(0, slot)).unwrap();
        }

        // Remove the middle value only; the other two remain.
        assert!(tree.remove(1, &key(7), &location(0, 5)).unwrap());
        assert_eq!(
            tree.scan_key(&key(7)).unwrap(),
            vec![location(0, 1), location(0, 9)]
        );
        // Removing a value that was never present (or already removed) is false.
        assert!(!tree.remove(1, &key(7), &location(0, 5)).unwrap());
        // Removing the remaining values empties the key.
        assert!(tree.remove(1, &key(7), &location(0, 1)).unwrap());
        assert!(tree.remove(1, &key(7), &location(0, 9)).unwrap());
        assert!(tree.scan_key(&key(7)).unwrap().is_empty());
        assert_eq!(tree.search(&key(7)).unwrap(), None);
    }

    #[test]
    fn failed_btree_insert_wal_append_does_not_leave_index_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(64, Box::new(AlwaysFlush), store));
        let wal = FailingWal::default();
        let registry = compress::CompressionRegistry::new();
        let tree = BTree::<RowLocation>::new(buffer.as_ref(), &wal, INDEX_FILE, &registry);
        tree.create(1).unwrap();

        wal.fail_next_append();
        let err = tree.insert(1, &key(9), &location(0, 9)).unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            tree.search(&key(9)).unwrap(),
            None,
            "failed WAL append left an unlogged index entry in the B-tree"
        );
    }

    #[test]
    fn failed_btree_create_root_append_does_not_leave_dirty_zero_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(64, Box::new(AlwaysFlush), store));
        let wal = FailingWal::default();
        let registry = compress::CompressionRegistry::new();
        let tree = BTree::<RowLocation>::new(buffer.as_ref(), &wal, INDEX_FILE, &registry);

        wal.fail_next_append();
        let err = tree.create(1).unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let dirty_zero_pages: Vec<_> = buffer
            .iter_pages()
            .unwrap()
            .filter(|page| {
                page.file_id == INDEX_FILE && page.is_dirty && !crate::page::is_valid(&page.data.0)
            })
            .map(|page| page.page_num)
            .collect();
        assert!(
            dirty_zero_pages.is_empty(),
            "failed create left dirty zero index pages: {dirty_zero_pages:?}"
        );
        assert_eq!(
            buffer.page_count(INDEX_FILE).unwrap(),
            0,
            "failed create advertised index pages with no redo base"
        );
    }

    #[test]
    fn failed_split_new_page_append_does_not_leave_dirty_invalid_page() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(64, Box::new(AlwaysFlush), store));
        let wal = FailingWal::default();
        let registry = compress::CompressionRegistry::new();
        let tree = BTree::<RowLocation>::new(buffer.as_ref(), &wal, INDEX_FILE, &registry);
        tree.create(1).unwrap();

        let mut value = 0i64;
        while !next_insert_would_split_root_leaf(
            buffer.as_ref(),
            INDEX_FILE,
            &fat_key(value),
            &location(value as PageNum, 0),
        ) {
            tree.insert(1, &fat_key(value), &location(value as PageNum, 0))
                .unwrap();
            value += 1;
            assert!(value < 20, "root leaf did not fill quickly");
        }
        buffer.mark_all_clean().unwrap();

        wal.fail_next_append();
        let err = tree
            .insert(1, &fat_key(value), &location(value as PageNum, 0))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let dirty_invalid: Vec<_> = buffer
            .iter_pages()
            .unwrap()
            .filter(|page| {
                page.file_id == INDEX_FILE && page.is_dirty && !crate::page::is_valid(&page.data.0)
            })
            .map(|page| page.page_num)
            .collect();
        assert!(
            dirty_invalid.is_empty(),
            "failed split left dirty invalid index pages: {dirty_invalid:?}"
        );
    }

    #[test]
    fn insert_after_failed_leaf_root_split_preserves_leaf_order() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(64, Box::new(AlwaysFlush), store));
        let wal = FailingWal::default();
        let registry = compress::CompressionRegistry::new();
        let tree = BTree::<RowLocation>::new(buffer.as_ref(), &wal, INDEX_FILE, &registry);
        tree.create(1).unwrap();

        let mut value = 0i64;
        while !next_insert_would_split_root_leaf(
            buffer.as_ref(),
            INDEX_FILE,
            &fat_key(value),
            &location(value as PageNum, 0),
        ) {
            tree.insert(1, &fat_key(value), &location(value as PageNum, 0))
                .unwrap();
            value += 1;
            assert!(value < 20, "root leaf did not fill quickly");
        }

        wal.fail_next_full_page_for_page(META_PAGE);
        let err = tree
            .insert(1, &fat_key(value), &location(value as PageNum, 0))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let later = value + 10;
        tree.insert(1, &fat_key(later), &location(later as PageNum, 0))
            .unwrap();

        let entries = tree.range(&KeyRange::All).unwrap();
        assert!(
            entries.iter().any(|(entry_key, value)| {
                entry_key == &fat_key(later) && *value == location(later as PageNum, 0)
            }),
            "later insert should remain reachable"
        );
        for pair in entries.windows(2) {
            assert!(
                pair[0].0 <= pair[1].0,
                "leaf chain order regressed after failed root split: {:?} before {:?}",
                pair[0].0,
                pair[1].0
            );
        }
    }

    #[test]
    fn failed_internal_root_split_metapage_append_preserves_existing_searches() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(128, Box::new(AlwaysFlush), store));
        let wal = FailingWal::default();
        let registry = compress::CompressionRegistry::new();
        let tree = BTree::<RowLocation>::new(buffer.as_ref(), &wal, INDEX_FILE, &registry);
        tree.create(1).unwrap();

        let mut inserted = Vec::new();
        let mut value = 0i64;
        loop {
            tree.insert(1, &fat_key(value), &location(value as PageNum, 0))
                .unwrap();
            inserted.push(value);
            let (_, is_leaf, entry_count) = root_shape(buffer.as_ref(), INDEX_FILE);
            if !is_leaf && entry_count >= 2 {
                break;
            }
            value += 1;
            assert!(value < 100, "root did not become an internal node quickly");
        }

        wal.fail_next_full_page_for_page(META_PAGE);
        let failed_value = loop {
            value += 1;
            match tree.insert(1, &fat_key(value), &location(value as PageNum, 0)) {
                Ok(()) => inserted.push(value),
                Err(err) => {
                    assert!(
                        err.message.contains("injected WAL append failure"),
                        "unexpected error: {err:?}"
                    );
                    break value;
                }
            }
            assert!(
                value < 200,
                "expected an internal root split to rewrite the metapage"
            );
        };

        for existing in inserted {
            assert_eq!(
                tree.search(&fat_key(existing)).unwrap(),
                Some(location(existing as PageNum, 0)),
                "lost key {existing} after failed root split while inserting {failed_value}"
            );
        }
    }

    #[test]
    fn failed_internal_root_left_append_preserves_existing_searches() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("idx")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(128, Box::new(AlwaysFlush), store));
        let wal = FailingWal::default();
        let registry = compress::CompressionRegistry::new();
        let tree = BTree::<RowLocation>::new(buffer.as_ref(), &wal, INDEX_FILE, &registry);
        tree.create(1).unwrap();

        let mut inserted = Vec::new();
        let mut value = 0i64;
        let root_to_fail = loop {
            tree.insert(1, &fat_key(value), &location(value as PageNum, 0))
                .unwrap();
            inserted.push(value);

            let (root, is_leaf, _) = root_shape(buffer.as_ref(), INDEX_FILE);
            if !is_leaf {
                let guard = buffer.read_page(INDEX_FILE, root).unwrap();
                let next_key = encode_key(&fat_key(value + 1)).unwrap();
                let next_value = location((value + 1) as PageNum, 0).encode().unwrap();
                let next_separator_len = next_key.len() + next_value.len();
                if !index_page::has_space(guard.data(), next_separator_len, CHILD_LEN) {
                    break root;
                }
            }

            value += 1;
            assert!(value < 200, "root did not become full quickly");
        };

        wal.fail_next_full_page_for_page(root_to_fail);
        let failed_value = loop {
            value += 1;
            match tree.insert(1, &fat_key(value), &location(value as PageNum, 0)) {
                Ok(()) => inserted.push(value),
                Err(err) => {
                    assert!(
                        err.message.contains("injected WAL append failure"),
                        "unexpected error: {err:?}"
                    );
                    break value;
                }
            }
            assert!(
                value < 300,
                "expected an internal root split to rewrite the old root"
            );
        };

        for existing in inserted {
            assert_eq!(
                tree.search(&fat_key(existing)).unwrap(),
                Some(location(existing as PageNum, 0)),
                "lost key {existing} after old-root rewrite failed while inserting {failed_value}"
            );
        }
    }

    #[test]
    fn equal_keys_scan_exactly_once_across_a_split() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Many entries on the SAME user-key, distinguished only by value, enough
        // to force several leaf splits and at least one root split — the
        // high-risk path where equal keys straddle a node boundary. Use distinct
        // page_nums (not just slot_num) as the value tiebreaker so the tree's
        // encoded-value byte order matches ascending numeric order. Insert in
        // shuffled value order so the tree must sort by value.
        let n = 600u32;
        let value_of = |page: u32| location(page, 0);
        let order: Vec<u32> = {
            let values: Vec<u32> = (0..n).collect();
            // Deterministic shuffle: interleave the two halves.
            let mut shuffled = Vec::with_capacity(n as usize);
            let (lo, hi) = values.split_at((n / 2) as usize);
            for index in 0..lo.len().max(hi.len()) {
                if let Some(value) = hi.get(index) {
                    shuffled.push(*value);
                }
                if let Some(value) = lo.get(index) {
                    shuffled.push(*value);
                }
            }
            shuffled
        };
        for page in &order {
            tree.insert(1, &key(42), &value_of(*page)).unwrap();
        }

        // The tree orders equal-key entries by encoded value bytes; page_num is
        // little-endian, and across 0..600 the low byte dominates the next, so
        // sort the expectation the same way the tree stores it.
        let mut expected: Vec<RowLocation> = (0..n).map(value_of).collect();
        expected.sort_by_key(|loc| loc.encode().unwrap());

        // Every value comes back exactly once, in encoded-value order.
        let scanned = tree.scan_key(&key(42)).unwrap();
        assert_eq!(scanned, expected);

        // A full range scan over the single key yields each entry exactly once,
        // in the same order.
        let ranged: Vec<RowLocation> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        assert_eq!(ranged, expected);
    }

    #[test]
    fn point_remove_targets_entries_in_later_leaves_of_a_dup_key_run() {
        // Milestone B's DELETE/UPDATE calls `remove(key, value)` to drop one
        // specific version's index entry. For a user-key whose entries span
        // several leaves after splits, the target may live in the 2nd or 3rd leaf
        // of the run, so the parent's `(key, value)` routing must descend past the
        // first leaf (including across a leaf boundary that is also an internal
        // separator) to reach it. Existing dup-key tests only cover
        // scan_key/range over such a run, never point remove/search of a mid/late
        // entry. This test forces a >=3-leaf run on one key and removes mid/late
        // and boundary entries, asserting EXACTLY the named entry is dropped and
        // no other entry is skipped, lost, or duplicated.
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Single user-key, many distinct values, built exactly like
        // `equal_keys_scan_exactly_once_across_a_split`: distinct page_nums as the
        // value tiebreaker (so encoded-value byte order is well defined), inserted
        // in a deterministic interleaved shuffle so the tree must sort by value.
        let dup_key = key(42);
        let n = 600u32;
        let value_of = |page: u32| location(page, 0);
        let order: Vec<u32> = {
            let values: Vec<u32> = (0..n).collect();
            let mut shuffled = Vec::with_capacity(n as usize);
            let (lo, hi) = values.split_at((n / 2) as usize);
            for index in 0..lo.len().max(hi.len()) {
                if let Some(value) = hi.get(index) {
                    shuffled.push(*value);
                }
                if let Some(value) = lo.get(index) {
                    shuffled.push(*value);
                }
            }
            shuffled
        };
        for page in &order {
            tree.insert(1, &dup_key, &value_of(*page)).unwrap();
        }

        // The tree stores equal-key entries in encoded-value order; mirror that.
        let expected_order = |present: &std::collections::BTreeSet<u32>| -> Vec<RowLocation> {
            let mut values: Vec<RowLocation> = present.iter().map(|p| value_of(*p)).collect();
            values.sort_by_key(|loc| loc.encode().unwrap());
            values
        };

        // Walk the leaf chain and group `dup_key`'s entries by the leaf they live
        // in, returning each leaf's page number alongside its values. This is how
        // we *prove* a target sits in the 2nd/3rd leaf of the run rather than the
        // first, so the test genuinely exercises the cross-leaf point path.
        let leaves_of_key = || -> Vec<(PageNum, Vec<RowLocation>)> {
            let buffer = fixture.buffer.as_ref();
            // Descend the leftmost spine to the first leaf, then follow links.
            let mut page_num = {
                let mut current =
                    index_page::meta_root(buffer.read_page(INDEX_FILE, META_PAGE).unwrap().data());
                loop {
                    let guard = buffer.read_page(INDEX_FILE, current).unwrap();
                    if index_page::is_leaf(guard.data()) {
                        break current;
                    }
                    current = index_page::link(guard.data());
                }
            };
            let mut leaves = Vec::new();
            loop {
                let guard = buffer.read_page(INDEX_FILE, page_num).unwrap();
                let data = guard.data();
                let count = index_page::entry_count(data);
                let mut here = Vec::new();
                for pos in 0..count {
                    let (entry_key, value) = leaf_key_value(data, pos).unwrap();
                    if entry_key == dup_key {
                        here.push(RowLocation::decode(value).unwrap());
                    }
                }
                if !here.is_empty() {
                    leaves.push((page_num, here));
                }
                let next = index_page::link(data);
                if next == 0 {
                    break;
                }
                page_num = next;
            }
            leaves
        };

        let leaves = leaves_of_key();
        // The run must span at least three leaves for this test to mean anything.
        assert!(
            leaves.len() >= 3,
            "expected the dup-key run to span >=3 leaves, got {}",
            leaves.len()
        );
        // Concatenating the per-leaf values must reproduce the full sorted scan
        // (sanity: our leaf walk sees every entry once, in order).
        let mut present: std::collections::BTreeSet<u32> = (0..n).collect();
        let flat_from_leaves: Vec<RowLocation> =
            leaves.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(flat_from_leaves, expected_order(&present));
        assert_eq!(tree.scan_key(&dup_key).unwrap(), expected_order(&present));

        // Recover the page-number tiebreaker from a stored RowLocation.
        let page_of = |loc: RowLocation| loc.page_num;

        // Pick concrete targets that, by the walk above, live in later leaves:
        //  - a value in the *middle* of the 2nd leaf of the run,
        //  - the *first* value of the 3rd leaf of the run; that value is exactly an
        //    internal separator, so the parent must route the probe *into* the 3rd
        //    leaf (a boundary case where mis-routing would land in the 2nd leaf),
        //  - a value in the *middle* of the 3rd leaf of the run.
        let second_leaf = &leaves[1].1;
        let third_leaf = &leaves[2].1;
        let target_second_mid = page_of(second_leaf[second_leaf.len() / 2]);
        let target_third_boundary = page_of(third_leaf[0]);
        let target_third_mid = page_of(third_leaf[third_leaf.len() / 2]);
        // Targets are distinct and are NOT the run's first (lowest) value, which
        // `search` returns; that guards against accidentally testing leaf 1.
        let lowest = page_of(leaves[0].1[0]);
        for t in [target_second_mid, target_third_boundary, target_third_mid] {
            assert_ne!(t, lowest, "target must not be the first-leaf/search value");
        }

        // Point reachability: `scan_key` reaches each later-leaf target, and
        // `search` returns the run's single lowest value (a first-leaf entry),
        // confirming the targets are genuinely past the first leaf.
        let scanned = tree.scan_key(&dup_key).unwrap();
        for t in [target_second_mid, target_third_boundary, target_third_mid] {
            assert!(
                scanned.contains(&value_of(t)),
                "scan_key did not reach later-leaf target page {t}"
            );
        }
        assert_eq!(tree.search(&dup_key).unwrap(), Some(value_of(lowest)));

        // Remove the 2nd-leaf entry. Exactly that entry must disappear; every
        // other value must remain exactly once, in order; the count drops by 1.
        let before = scanned.len();
        assert!(
            tree.remove(1, &dup_key, &value_of(target_second_mid))
                .unwrap()
        );
        present.remove(&target_second_mid);
        let after = tree.scan_key(&dup_key).unwrap();
        assert_eq!(
            after,
            expected_order(&present),
            "after removing 2nd-leaf entry"
        );
        assert_eq!(after.len(), before - 1, "exactly one entry removed");
        assert!(!after.contains(&value_of(target_second_mid)));

        // Re-removing the same (now-absent) entry is a no-op returning false and
        // must not perturb the multiset — catches a stray double-remove.
        assert!(
            !tree
                .remove(1, &dup_key, &value_of(target_second_mid))
                .unwrap()
        );
        assert_eq!(tree.scan_key(&dup_key).unwrap(), expected_order(&present));

        // Remove the 3rd-leaf boundary entry (a leaf/internal-separator boundary).
        let before = present.len();
        assert!(
            tree.remove(1, &dup_key, &value_of(target_third_boundary))
                .unwrap()
        );
        present.remove(&target_third_boundary);
        let after = tree.scan_key(&dup_key).unwrap();
        assert_eq!(
            after,
            expected_order(&present),
            "after removing 3rd-leaf boundary"
        );
        assert_eq!(after.len(), before - 1, "exactly one entry removed");
        assert!(!after.contains(&value_of(target_third_boundary)));

        // Remove a 3rd-leaf middle entry for good measure.
        let before = present.len();
        assert!(
            tree.remove(1, &dup_key, &value_of(target_third_mid))
                .unwrap()
        );
        present.remove(&target_third_mid);
        let after = tree.scan_key(&dup_key).unwrap();
        assert_eq!(
            after,
            expected_order(&present),
            "after removing 3rd-leaf middle"
        );
        assert_eq!(after.len(), before - 1, "exactly one entry removed");
        assert!(!after.contains(&value_of(target_third_mid)));

        // A value that was never present removes nothing and reports false.
        assert!(!tree.remove(1, &dup_key, &value_of(n + 7)).unwrap());
        assert_eq!(
            tree.scan_key(&dup_key).unwrap(),
            expected_order(&present),
            "absent-value remove must not change the run"
        );

        // Full-range scan over the single key still yields each survivor once.
        let ranged: Vec<RowLocation> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        assert_eq!(ranged, expected_order(&present));
    }

    #[test]
    fn near_equal_keys_across_split_preserve_order() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // A mix of duplicate and distinct keys spanning many splits: keys 0..50,
        // each with several values. Insert reversed so the tree sorts everything.
        let keys = 50i64;
        let dups = 12u16;
        for k in (0..keys).rev() {
            for slot in (0..dups).rev() {
                tree.insert(1, &key(k), &location(k as u32, slot)).unwrap();
            }
        }

        // The full range must list every (key, value) exactly once in order.
        let entries = tree.range(&KeyRange::All).unwrap();
        let mut expected = Vec::new();
        for k in 0..keys {
            for slot in 0..dups {
                expected.push((key(k), location(k as u32, slot)));
            }
        }
        assert_eq!(entries, expected);

        // Per-key scans also return exactly the right values.
        for k in 0..keys {
            let values = tree.scan_key(&key(k)).unwrap();
            let want: Vec<RowLocation> = (0..dups).map(|slot| location(k as u32, slot)).collect();
            assert_eq!(values, want, "key {k}");
        }
    }

    #[test]
    fn range_scan_with_duplicates_returns_all_entries() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        for k in 0..20i64 {
            for slot in 0..3u16 {
                tree.insert(1, &key(k), &location(k as u32, slot)).unwrap();
            }
        }

        let bounded = tree
            .range(&KeyRange::Range {
                start: Bound::Included(key(10)),
                end: Bound::Excluded(key(13)),
            })
            .unwrap();
        let mut expected = Vec::new();
        for k in 10..13i64 {
            for slot in 0..3u16 {
                expected.push((key(k), location(k as u32, slot)));
            }
        }
        assert_eq!(bounded, expected);
    }

    #[test]
    fn update_via_remove_then_reinsert() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        tree.insert(1, &key(1), &location(0, 0)).unwrap();

        // The engine updates the PK location by removing the old (key, value) and
        // inserting the new one; verify that primitive works.
        assert!(tree.remove(1, &key(1), &location(0, 0)).unwrap());
        tree.insert(1, &key(1), &location(3, 7)).unwrap();
        assert_eq!(tree.search(&key(1)).unwrap(), Some(location(3, 7)));
    }

    #[test]
    fn many_inserts_split_and_remain_searchable() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Enough keys to force multiple leaf splits and at least one root split.
        let n = 500i64;
        for value in 0..n {
            tree.insert(1, &key(value), &location(value as u32, 0))
                .unwrap();
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
            assert!(
                tree.remove(1, &key(value), &location(value as u32, 0))
                    .unwrap()
            );
        }
        for value in 0..300i64 {
            let expected = (value % 2 != 0).then(|| location(value as u32, 0));
            assert_eq!(tree.search(&key(value)).unwrap(), expected);
        }
        // A removed key can be reinserted.
        tree.insert(1, &key(0), &location(99, 1)).unwrap();
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
            tree.insert(1, &text_key(value), &location(value as u32, 0))
                .unwrap();
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
    fn stores_heap_tids_for_a_secondary_index() {
        let fixture = Fixture::new(64);
        let tree = fixture.secondary_tree();
        tree.create(1).unwrap();

        // Secondary-index shape (uniform with the primary key): key = [indexed_value]
        // alone, value = heap TID. The same indexed value (10) appears twice,
        // disambiguated by the trailing TID in `(key, tid)` order.
        let indexed = |value: i64| Key(vec![Value::Integer(value)]);
        for (value, slot) in [(20, 3u16), (10, 1), (10, 2)] {
            tree.insert(1, &indexed(value), &location(0, slot)).unwrap();
        }

        // A point scan of indexed value 10 returns both TIDs in `(key, tid)` order.
        assert_eq!(
            tree.scan_key(&indexed(10)).unwrap(),
            vec![location(0, 1), location(0, 2)]
        );

        // Range order follows (indexed value, tid), so TIDs come back 1, 2, 3.
        let tids: Vec<_> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, tid)| tid)
            .collect();
        assert_eq!(tids, vec![location(0, 1), location(0, 2), location(0, 3)]);
    }

    #[test]
    fn range_matches_indexed_prefix_disambiguated_by_tid() {
        let fixture = Fixture::new(64);
        let tree = fixture.secondary_tree();
        tree.create(1).unwrap();

        // key = [indexed value] alone; equal indexed values share a key and are
        // disambiguated by the heap TID (the leaf value), not an embedded pk.
        for (indexed, slot) in [(10, 1u16), (10, 5), (20, 2), (30, 3)] {
            tree.insert(1, &Key(vec![Value::Integer(indexed)]), &location(0, slot))
                .unwrap();
        }
        let slots = |entries: Vec<(Key, RowLocation)>| -> Vec<u16> {
            entries.into_iter().map(|(_, tid)| tid.slot_num).collect()
        };

        // Equality on the indexed value returns every row sharing it (both TIDs).
        let eq = tree
            .range(&KeyRange::Exact(Key(vec![Value::Integer(10)])))
            .unwrap();
        assert_eq!(slots(eq), vec![1, 5]);

        // An inclusive bound on the indexed value still includes its rows.
        let inclusive = tree
            .range(&KeyRange::Range {
                start: Bound::Included(Key(vec![Value::Integer(20)])),
                end: Bound::Included(Key(vec![Value::Integer(20)])),
            })
            .unwrap();
        assert_eq!(slots(inclusive), vec![2]);

        // A half-open range over the indexed value.
        let bounded = tree
            .range(&KeyRange::Range {
                start: Bound::Included(Key(vec![Value::Integer(10)])),
                end: Bound::Excluded(Key(vec![Value::Integer(30)])),
            })
            .unwrap();
        assert_eq!(slots(bounded), vec![1, 5, 2]);
    }

    #[test]
    fn remove_values_in_drops_exactly_the_dead_tids() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // Distinct keys, one entry each. Mark a scattered subset of TIDs dead.
        let n = 40i64;
        for value in 0..n {
            tree.insert(1, &key(value), &location(value as u32, 0))
                .unwrap();
        }
        let dead: std::collections::HashSet<RowLocation> = (0..n)
            .filter(|v| v % 3 == 0)
            .map(|v| location(v as u32, 0))
            .collect();

        let removed = tree.remove_values_in(1, &dead).unwrap();
        assert_eq!(removed, dead.len());

        // No surviving entry resolves to a dead TID; every live TID is still present.
        let surviving: Vec<RowLocation> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        for value in &surviving {
            assert!(!dead.contains(value), "{value:?} should have been removed");
        }
        let expected: Vec<RowLocation> = (0..n)
            .filter(|v| v % 3 != 0)
            .map(|v| location(v as u32, 0))
            .collect();
        assert_eq!(surviving, expected);
        for value in 0..n {
            let want = (value % 3 != 0).then(|| location(value as u32, 0));
            assert_eq!(tree.search(&key(value)).unwrap(), want, "key {value}");
        }
    }

    #[test]
    fn remove_values_in_spans_multiple_leaves_and_dup_key_runs() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();

        // One user-key, many distinct values, built like
        // `point_remove_targets_entries_in_later_leaves_of_a_dup_key_run` so the run
        // spans several leaves and dead TIDs land in the middle and at leaf
        // boundaries of the run.
        let dup_key = key(42);
        let n = 600u32;
        let value_of = |page: u32| location(page, 0);
        let order: Vec<u32> = {
            let values: Vec<u32> = (0..n).collect();
            let mut shuffled = Vec::with_capacity(n as usize);
            let (lo, hi) = values.split_at((n / 2) as usize);
            for index in 0..lo.len().max(hi.len()) {
                if let Some(value) = hi.get(index) {
                    shuffled.push(*value);
                }
                if let Some(value) = lo.get(index) {
                    shuffled.push(*value);
                }
            }
            shuffled
        };
        for page in &order {
            tree.insert(1, &dup_key, &value_of(*page)).unwrap();
        }

        // Also a couple of distinct neighbor keys with dead and live entries, so the
        // pass crosses leaves carrying more than one key.
        for value in [700u32, 800] {
            tree.insert(1, &key(value as i64), &value_of(value))
                .unwrap();
        }

        // Mark every even page_num (within the dup run) plus one neighbor dead.
        let dead: std::collections::HashSet<RowLocation> = (0..n)
            .filter(|p| p % 2 == 0)
            .chain(std::iter::once(700))
            .map(value_of)
            .collect();

        let removed = tree.remove_values_in(1, &dead).unwrap();
        assert_eq!(removed, dead.len());

        // The dup-key run now holds exactly the odd page_nums, each once, in order.
        let mut expected: Vec<RowLocation> = (0..n).filter(|p| p % 2 != 0).map(value_of).collect();
        expected.sort_by_key(|loc| loc.encode().unwrap());
        assert_eq!(tree.scan_key(&dup_key).unwrap(), expected);

        // A full range scan returns every survivor exactly once and never a dead TID.
        let all: Vec<RowLocation> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        for value in &all {
            assert!(!dead.contains(value), "{value:?} should have been removed");
        }
        // Neighbor key 700 was dead (gone), 800 survives.
        assert!(tree.scan_key(&key(700)).unwrap().is_empty());
        assert_eq!(tree.scan_key(&key(800)).unwrap(), vec![value_of(800)]);

        // Idempotent: re-running with the same set removes nothing more.
        assert_eq!(tree.remove_values_in(1, &dead).unwrap(), 0);
        assert_eq!(tree.scan_key(&dup_key).unwrap(), expected);
    }

    #[test]
    fn remove_values_in_empty_set_is_a_noop() {
        let fixture = Fixture::new(64);
        let tree = fixture.tree();
        tree.create(1).unwrap();
        for value in 0..10i64 {
            tree.insert(1, &key(value), &location(value as u32, 0))
                .unwrap();
        }
        let empty: std::collections::HashSet<RowLocation> = std::collections::HashSet::new();
        assert_eq!(tree.remove_values_in(1, &empty).unwrap(), 0);
        let all: Vec<RowLocation> = tree
            .range(&KeyRange::All)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        assert_eq!(
            all,
            (0..10).map(|v| location(v as u32, 0)).collect::<Vec<_>>()
        );
    }
}
