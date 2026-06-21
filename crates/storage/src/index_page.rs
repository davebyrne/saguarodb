//! B-tree node page layout for the on-disk primary-key index.
//!
//! A node reuses the shared 22-byte page header (`page::*`), so it gets the same
//! page-id / version / page-LSN / checksum machinery and torn-page protection as
//! a heap page. Immediately after the header sits a 5-byte node sub-header:
//!
//! ```text
//! [is_leaf: 1][link: u32]
//! ```
//!
//! `link` is the right-sibling page for a leaf (0 = none) and the leftmost child
//! page for an internal node (the child for keys ordered before the first
//! separator). Entries follow a slotted layout like the heap page, but the slots
//! are kept sorted by key and carry no dead flag (a delete removes the slot):
//!
//! - Slots grow down from the end of the page, 4 bytes each: `[offset: u16][len: u16]`.
//! - Entry bytes grow up from the body start: `[key_len: u16][key][value]`.
//!
//! A leaf entry's value is an encoded `RowLocation`; an internal entry's value is
//! a child `PageNum`. The btree (`btree.rs`) owns key comparison and the tree
//! structure; this module only manipulates the on-page bytes.

use buffer::PAGE_SIZE;
use common::{PageNum, Result};

use crate::page::{
    FREE_SPACE_OFFSET, HEADER_LEN, NUM_SLOTS_OFFSET, PAGE_ID_OFFSET, PAGE_TYPE_INDEX,
    PAGE_TYPE_OFFSET, PAGE_VERSION, PAGE_VERSION_OFFSET, corrupt_page, read_u16, read_u32,
    write_checksum, write_u16, write_u32,
};

const IS_LEAF_OFFSET: usize = HEADER_LEN;
const LINK_OFFSET: usize = HEADER_LEN + 1;
const BODY_START: usize = HEADER_LEN + 5;
const SLOT_LEN: usize = 4;

/// Initialize a fresh node page (leaf or internal). The page-LSN is left zero;
/// the caller stamps it after logging the corresponding redo record.
pub(crate) fn init(data: &mut [u8; PAGE_SIZE], page_id: PageNum, is_leaf: bool) {
    data.fill(0);
    write_u32(data, PAGE_ID_OFFSET, page_id);
    data[PAGE_TYPE_OFFSET] = PAGE_TYPE_INDEX;
    data[PAGE_VERSION_OFFSET] = PAGE_VERSION;
    write_u16(data, NUM_SLOTS_OFFSET, 0);
    write_u16(data, FREE_SPACE_OFFSET, BODY_START as u16);
    data[IS_LEAF_OFFSET] = u8::from(is_leaf);
    write_u32(data, LINK_OFFSET, 0);
    write_checksum(data);
}

pub(crate) fn is_leaf(data: &[u8; PAGE_SIZE]) -> bool {
    data[IS_LEAF_OFFSET] == 1
}

/// Right-sibling page for a leaf, or leftmost child for an internal node.
pub(crate) fn link(data: &[u8; PAGE_SIZE]) -> PageNum {
    read_u32(data, LINK_OFFSET)
}

pub(crate) fn set_link(data: &mut [u8; PAGE_SIZE], page: PageNum) {
    write_u32(data, LINK_OFFSET, page);
    write_checksum(data);
}

pub(crate) fn entry_count(data: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(data, NUM_SLOTS_OFFSET)
}

fn slot_pos(index: u16) -> usize {
    PAGE_SIZE - (index as usize + 1) * SLOT_LEN
}

fn read_slot(data: &[u8; PAGE_SIZE], index: u16) -> (usize, usize) {
    let pos = slot_pos(index);
    (
        read_u16(data, pos) as usize,
        read_u16(data, pos + 2) as usize,
    )
}

fn write_slot(data: &mut [u8; PAGE_SIZE], index: u16, offset: usize, len: usize) {
    let pos = slot_pos(index);
    write_u16(data, pos, offset as u16);
    write_u16(data, pos + 2, len as u16);
}

/// Key bytes of entry `index` (borrowed from the page).
pub(crate) fn entry_key(data: &[u8; PAGE_SIZE], index: u16) -> &[u8] {
    let (offset, _len) = read_slot(data, index);
    let key_len = read_u16(data, offset) as usize;
    &data[offset + 2..offset + 2 + key_len]
}

/// Value bytes of entry `index` (RowLocation for a leaf, child PageNum for an
/// internal node), borrowed from the page.
pub(crate) fn entry_value(data: &[u8; PAGE_SIZE], index: u16) -> &[u8] {
    let (offset, len) = read_slot(data, index);
    let key_len = read_u16(data, offset) as usize;
    &data[offset + 2 + key_len..offset + len]
}

fn free_bytes(data: &[u8; PAGE_SIZE]) -> usize {
    let free_start = read_u16(data, FREE_SPACE_OFFSET) as usize;
    let count = entry_count(data) as usize;
    let slots_bottom = PAGE_SIZE - count * SLOT_LEN;
    slots_bottom.saturating_sub(free_start)
}

/// Whether one more entry of the given key/value size fits (entry bytes plus a
/// new slot).
pub(crate) fn has_space(data: &[u8; PAGE_SIZE], key_len: usize, value_len: usize) -> bool {
    free_bytes(data) >= entry_stored_len(key_len, value_len)
}

fn entry_size(key_len: usize, value_len: usize) -> usize {
    2 + key_len + value_len
}

/// Total on-page footprint of an entry: its bytes plus its slot. The btree uses
/// this to choose a byte-balanced split point for variable-length keys.
pub(crate) fn entry_stored_len(key_len: usize, value_len: usize) -> usize {
    entry_size(key_len, value_len) + SLOT_LEN
}

/// Insert an entry at logical position `pos` (0..=count), shifting later slots.
/// The caller is responsible for keeping entries sorted by key. Fails if the
/// node is full; the btree splits before reaching that.
pub(crate) fn insert_entry(
    data: &mut [u8; PAGE_SIZE],
    pos: u16,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    let count = entry_count(data);
    if pos > count {
        return Err(corrupt_page("index insert position out of range"));
    }
    if !has_space(data, key.len(), value.len()) {
        return Err(corrupt_page("index node is full"));
    }

    let free_start = read_u16(data, FREE_SPACE_OFFSET) as usize;
    let size = entry_size(key.len(), value.len());
    write_u16(data, free_start, key.len() as u16);
    data[free_start + 2..free_start + 2 + key.len()].copy_from_slice(key);
    data[free_start + 2 + key.len()..free_start + size].copy_from_slice(value);

    for index in (pos..count).rev() {
        let (offset, len) = read_slot(data, index);
        write_slot(data, index + 1, offset, len);
    }
    write_slot(data, pos, free_start, size);
    write_u16(data, NUM_SLOTS_OFFSET, count + 1);
    write_u16(data, FREE_SPACE_OFFSET, (free_start + size) as u16);
    write_checksum(data);
    Ok(())
}

/// Remove the entry at `pos`, shifting later slots down. The entry bytes are not
/// reclaimed (accepted bloat); a later `truncate` compacts them.
pub(crate) fn remove_entry(data: &mut [u8; PAGE_SIZE], pos: u16) -> Result<()> {
    let count = entry_count(data);
    if pos >= count {
        return Err(corrupt_page("index remove position out of range"));
    }
    for index in pos + 1..count {
        let (offset, len) = read_slot(data, index);
        write_slot(data, index - 1, offset, len);
    }
    write_u16(data, NUM_SLOTS_OFFSET, count - 1);
    write_checksum(data);
    Ok(())
}

// --- Metapage (page 0 of an index file): holds the current root page number ---

const META_ROOT_OFFSET: usize = HEADER_LEN;

pub(crate) fn meta_init(data: &mut [u8; PAGE_SIZE], page_id: PageNum, root: PageNum) {
    data.fill(0);
    write_u32(data, PAGE_ID_OFFSET, page_id);
    data[PAGE_TYPE_OFFSET] = PAGE_TYPE_INDEX;
    data[PAGE_VERSION_OFFSET] = PAGE_VERSION;
    write_u32(data, META_ROOT_OFFSET, root);
    write_checksum(data);
}

pub(crate) fn meta_root(data: &[u8; PAGE_SIZE]) -> PageNum {
    read_u32(data, META_ROOT_OFFSET)
}

pub(crate) fn meta_set_root(data: &mut [u8; PAGE_SIZE], root: PageNum) {
    write_u32(data, META_ROOT_OFFSET, root);
    write_checksum(data);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page;
    use buffer::PageData;

    fn leaf() -> PageData {
        let mut data = PageData::default();
        init(&mut data.0, 1, true);
        data
    }

    #[test]
    fn fresh_leaf_is_valid_and_empty() {
        let data = leaf();
        assert!(page::is_valid(&data.0));
        assert!(is_leaf(&data.0));
        assert_eq!(entry_count(&data.0), 0);
        assert_eq!(link(&data.0), 0);
    }

    #[test]
    fn internal_node_is_not_a_leaf() {
        let mut data = PageData::default();
        init(&mut data.0, 2, false);
        assert!(page::is_valid(&data.0));
        assert!(!is_leaf(&data.0));
    }

    #[test]
    fn insert_keeps_entries_at_chosen_positions() {
        let mut data = leaf();
        // Insert out of order but at sorted positions, as the btree would.
        insert_entry(&mut data.0, 0, b"b", b"2").unwrap();
        insert_entry(&mut data.0, 0, b"a", b"1").unwrap();
        insert_entry(&mut data.0, 2, b"c", b"3").unwrap();

        assert_eq!(entry_count(&data.0), 3);
        assert_eq!(entry_key(&data.0, 0), b"a");
        assert_eq!(entry_value(&data.0, 0), b"1");
        assert_eq!(entry_key(&data.0, 1), b"b");
        assert_eq!(entry_key(&data.0, 2), b"c");
        assert!(page::is_valid(&data.0));
    }

    #[test]
    fn remove_shifts_later_entries() {
        let mut data = leaf();
        insert_entry(&mut data.0, 0, b"a", b"1").unwrap();
        insert_entry(&mut data.0, 1, b"b", b"2").unwrap();
        insert_entry(&mut data.0, 2, b"c", b"3").unwrap();

        remove_entry(&mut data.0, 1).unwrap();

        assert_eq!(entry_count(&data.0), 2);
        assert_eq!(entry_key(&data.0, 0), b"a");
        assert_eq!(entry_key(&data.0, 1), b"c");
    }

    #[test]
    fn has_space_reports_full_node() {
        let mut data = leaf();
        let big = vec![7u8; PAGE_SIZE / 2];
        assert!(has_space(&data.0, big.len(), 1));
        insert_entry(&mut data.0, 0, &big, b"v").unwrap();
        // A second half-page entry cannot fit.
        assert!(!has_space(&data.0, big.len(), 1));
        let err = insert_entry(&mut data.0, 1, &big, b"v").unwrap_err();
        assert!(err.message.contains("full"));
    }
}
