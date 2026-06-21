use buffer::PAGE_SIZE;
use common::{DbError, Lsn, PageNum, Result, SqlState};

pub const PAGE_TYPE_DATA: u8 = 1;
pub(crate) const PAGE_TYPE_INDEX: u8 = 2;
pub(crate) const PAGE_VERSION: u8 = 2;

pub(crate) const HEADER_LEN: usize = 22;
pub(crate) const PAGE_ID_OFFSET: usize = 0;
pub(crate) const PAGE_TYPE_OFFSET: usize = 4;
pub(crate) const PAGE_VERSION_OFFSET: usize = 5;
pub(crate) const NUM_SLOTS_OFFSET: usize = 6;
pub(crate) const FREE_SPACE_OFFSET: usize = 8;
const PAGE_LSN_OFFSET: usize = 10;
const CHECKSUM_OFFSET: usize = 18;
pub(crate) const SLOT_LEN: usize = 6;

/// Line-pointer (ItemId) states stored in a heap slot's `flags` field (§5.2 of
/// `mvcc.md`). A heap slot is a *line pointer*: a stable `(page, slot)` address
/// that an index entry references; the tuple bytes it names may later be
/// relocated within the page (compaction, Milestone F) by rewriting the line
/// pointer's `(offset, len)` without touching any index. The slot id is stable
/// across that relocation, which is the contract `RowId`/`RowLocation` rely on.
///
/// The numeric values preserve the pre-MVCC `SLOT_DEAD = 1` / `SLOT_LIVE = 2`
/// encoding, so this is a pure renaming: today's "live" slot is `NORMAL` and
/// today's tombstoned slot is `DEAD`. `UNUSED` and `REDIRECT` are reserved for
/// later milestones and not yet produced by any path.
mod line_pointer {
    /// `(offset, len)` address a live tuple on this page (today's "live" slot).
    pub(super) const NORMAL: u16 = 2;
    /// Tuple removed; the line pointer is retained because index entries may
    /// still reference it (today's tombstoned slot). Reclaimed to `UNUSED` only
    /// after index vacuum.
    pub(super) const DEAD: u16 = 1;
    /// Free for reuse. Defined now; reclaim (`DEAD`/`REDIRECT` -> `UNUSED`) is
    /// owned by VACUUM (Milestone F), so nothing assigns it yet.
    #[allow(dead_code, reason = "line-pointer reclaim owned by Milestone F")]
    pub(super) const UNUSED: u16 = 0;
    /// Points at another slot on the same page. Reserved for HOT (Milestone H);
    /// no path produces it yet.
    #[allow(
        dead_code,
        reason = "REDIRECT line pointers owned by HOT (Milestone H)"
    )]
    pub(super) const REDIRECT: u16 = 3;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHeader {
    num_slots: u16,
    free_start: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Slot {
    offset: u16,
    len: u16,
    flags: u16,
}

impl Slot {
    /// True when this line pointer is `NORMAL` (addresses a live tuple).
    fn is_live(self) -> bool {
        self.flags == line_pointer::NORMAL
    }
}

pub fn init_page(data: &mut [u8; PAGE_SIZE], page_id: PageNum) {
    data.fill(0);
    write_u32(data, PAGE_ID_OFFSET, page_id);
    data[PAGE_TYPE_OFFSET] = PAGE_TYPE_DATA;
    data[PAGE_VERSION_OFFSET] = PAGE_VERSION;
    write_u16(data, NUM_SLOTS_OFFSET, 0);
    write_u16(data, FREE_SPACE_OFFSET, HEADER_LEN as u16);
    write_checksum(data);
}

pub fn validate(data: &[u8; PAGE_SIZE]) -> Result<PageHeader> {
    let page_type = data[PAGE_TYPE_OFFSET];
    if page_type != PAGE_TYPE_DATA && page_type != PAGE_TYPE_INDEX {
        return Err(corrupt_page("unexpected page type"));
    }
    if data[PAGE_VERSION_OFFSET] != PAGE_VERSION {
        return Err(corrupt_page(format!(
            "unsupported page version {}",
            data[PAGE_VERSION_OFFSET]
        )));
    }
    let stored_checksum = read_u32(data, CHECKSUM_OFFSET);
    if stored_checksum != checksum(data) {
        return Err(corrupt_page("page checksum mismatch"));
    }

    let header = PageHeader {
        num_slots: read_u16(data, NUM_SLOTS_OFFSET),
        free_start: read_u16(data, FREE_SPACE_OFFSET),
    };
    // Index nodes carry their own (sorted-slot) body layout validated by the
    // btree; here the shared version + checksum are enough to trust the page.
    if page_type == PAGE_TYPE_DATA {
        validate_layout(data, header)?;
    }
    Ok(header)
}

pub fn is_initialized(data: &[u8; PAGE_SIZE]) -> bool {
    data[PAGE_TYPE_OFFSET] == PAGE_TYPE_DATA
}

/// Stamp the page-LSN (the LSN of the WAL record that last modified this page)
/// into the header and refresh the checksum.
pub fn set_page_lsn(data: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    write_u64(data, PAGE_LSN_OFFSET, lsn);
    write_checksum(data);
}

/// Read the page-LSN without validating the page. Safe on freshly zeroed or
/// not-yet-initialized buffers, which redo gating relies on.
pub fn page_lsn(data: &[u8; PAGE_SIZE]) -> Lsn {
    read_u64(data, PAGE_LSN_OFFSET)
}

/// The slot number a subsequent `insert_row` will assign (the current slot count).
pub fn next_slot(data: &[u8; PAGE_SIZE]) -> Result<u16> {
    Ok(validate(data)?.num_slots)
}

/// Whether a page buffer is a structurally valid, checksum-correct data page.
/// Recovery uses this to detect torn/uninitialized pages before redo.
pub fn is_valid(data: &[u8; PAGE_SIZE]) -> bool {
    validate(data).is_ok()
}

pub fn has_space_for(data: &[u8; PAGE_SIZE], row_len: usize) -> Result<bool> {
    let header = validate(data)?;
    Ok(free_bytes(header) >= row_len)
}

pub fn insert_row(data: &mut [u8; PAGE_SIZE], row: &[u8]) -> Result<u16> {
    let header = validate(data)?;
    let row_len = u16::try_from(row.len())
        .map_err(|_| DbError::storage(SqlState::InternalError, "row is too large"))?;
    if free_bytes(header) < row.len() {
        return Err(DbError::storage(
            SqlState::InternalError,
            "page does not have enough free space",
        ));
    }

    let slot_num = header.num_slots;
    let row_offset = header.free_start;
    let row_end = row_offset as usize + row.len();
    data[row_offset as usize..row_end].copy_from_slice(row);
    write_slot(
        data,
        slot_num,
        Slot {
            offset: row_offset,
            len: row_len,
            flags: line_pointer::NORMAL,
        },
    );
    write_u16(data, NUM_SLOTS_OFFSET, slot_num + 1);
    write_u16(data, FREE_SPACE_OFFSET, row_offset + row_len);
    write_checksum(data);
    Ok(slot_num)
}

pub fn read_row(data: &[u8; PAGE_SIZE], slot_num: u16) -> Result<Option<Vec<u8>>> {
    let header = validate(data)?;
    if slot_num >= header.num_slots {
        return Err(corrupt_page("slot number is out of bounds"));
    }
    let slot = read_slot(data, slot_num);
    if !slot.is_live() {
        return Ok(None);
    }
    let start = slot.offset as usize;
    let end = start + slot.len as usize;
    Ok(Some(data[start..end].to_vec()))
}

pub fn delete_row(data: &mut [u8; PAGE_SIZE], slot_num: u16) -> Result<bool> {
    let header = validate(data)?;
    if slot_num >= header.num_slots {
        return Err(corrupt_page("slot number is out of bounds"));
    }
    let mut slot = read_slot(data, slot_num);
    if !slot.is_live() {
        return Ok(false);
    }
    slot.flags = line_pointer::DEAD;
    write_slot(data, slot_num, slot);
    write_checksum(data);
    Ok(true)
}

/// Mutate the MVCC header (`xmax`, `t_ctid`, `infomask`) of the live tuple at
/// `slot_num` **in place**, stamp the page-LSN, and refresh the checksum — the
/// substrate for `UPDATE`/`DELETE` version stamping (Milestone B commits 8–9).
///
/// These three are fixed-width header fields, so the tuple keeps its exact
/// length and offset: nothing is relocated and the page is not compacted. The
/// header offsets live in `codec::set_mvcc_header_fields`, called here on the
/// slot's existing byte range, so layout stays DRY in `codec`. PageLSN/checksum
/// are refreshed exactly like `insert_row`/`delete_row` (the `lsn` is the LSN of
/// the WAL record that authorizes the change; the `HeapUpdateHeader` record and
/// its emission are later commits, so a unit test may pass a synthetic LSN).
///
/// The line pointer must be `NORMAL` (live); a dead/unused/out-of-bounds slot is
/// a misuse and returns a structured `DbError` rather than panicking, matching
/// the sibling primitives.
///
/// Its first caller is `apply_physical_redo` (the `HeapUpdateHeader` redo arm);
/// the engine's `UPDATE`/`DELETE` emission paths arrive in Milestone B commits
/// 8–9.
pub fn set_tuple_header(
    data: &mut [u8; PAGE_SIZE],
    slot_num: u16,
    xmax: common::TxnId,
    t_ctid: (PageNum, u16),
    infomask: u16,
    lsn: Lsn,
) -> Result<()> {
    let header = validate(data)?;
    if slot_num >= header.num_slots {
        return Err(corrupt_page("slot number is out of bounds"));
    }
    let slot = read_slot(data, slot_num);
    if !slot.is_live() {
        return Err(DbError::storage(
            SqlState::InternalError,
            "cannot mutate the header of a non-live slot",
        ));
    }
    let start = slot.offset as usize;
    let end = start + slot.len as usize;
    crate::codec::set_mvcc_header_fields(&mut data[start..end], xmax, t_ctid, infomask)?;
    set_page_lsn(data, lsn);
    Ok(())
}

fn validate_layout(data: &[u8; PAGE_SIZE], header: PageHeader) -> Result<()> {
    if header.free_start as usize > PAGE_SIZE {
        return Err(corrupt_page("free space offset is outside page"));
    }
    if (header.free_start as usize) < HEADER_LEN {
        return Err(corrupt_page("free space offset overlaps header"));
    }

    let slot_start = if header.num_slots == 0 {
        PAGE_SIZE
    } else {
        slot_offset(header.num_slots - 1).ok_or_else(|| corrupt_page("too many slots"))?
    };
    if header.free_start as usize > slot_start {
        return Err(corrupt_page("row data overlaps slot array"));
    }

    for slot_num in 0..header.num_slots {
        let slot = read_slot(data, slot_num);
        // Only NORMAL and DEAD line pointers are produced in this milestone;
        // UNUSED/REDIRECT (reclaim/HOT) are reserved and not yet written, so a
        // page carrying any other flag value is corrupt.
        if slot.flags != line_pointer::NORMAL && slot.flags != line_pointer::DEAD {
            return Err(corrupt_page("slot has invalid flags"));
        }
        let start = slot.offset as usize;
        let end = start
            .checked_add(slot.len as usize)
            .ok_or_else(|| corrupt_page("slot length overflows"))?;
        if start < HEADER_LEN || end > header.free_start as usize {
            return Err(corrupt_page("slot points outside row region"));
        }
    }

    Ok(())
}

fn free_bytes(header: PageHeader) -> usize {
    slot_offset(header.num_slots)
        .unwrap_or(0)
        .saturating_sub(header.free_start as usize)
}

fn slot_offset(slot_num: u16) -> Option<usize> {
    PAGE_SIZE.checked_sub((slot_num as usize + 1) * SLOT_LEN)
}

fn read_slot(data: &[u8; PAGE_SIZE], slot_num: u16) -> Slot {
    let offset = slot_offset(slot_num).expect("slot offset already validated");
    Slot {
        offset: read_u16(data, offset),
        len: read_u16(data, offset + 2),
        flags: read_u16(data, offset + 4),
    }
}

fn write_slot(data: &mut [u8; PAGE_SIZE], slot_num: u16, slot: Slot) {
    let offset = slot_offset(slot_num).expect("slot offset already validated");
    write_u16(data, offset, slot.offset);
    write_u16(data, offset + 2, slot.len);
    write_u16(data, offset + 4, slot.flags);
}

fn checksum(data: &[u8; PAGE_SIZE]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data[..CHECKSUM_OFFSET]);
    hasher.update(&[0; 4]);
    hasher.update(&data[CHECKSUM_OFFSET + 4..]);
    hasher.finalize()
}

pub(crate) fn write_checksum(data: &mut [u8; PAGE_SIZE]) {
    write_u32(data, CHECKSUM_OFFSET, checksum(data));
}

pub(crate) fn read_u16(data: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

pub(crate) fn write_u16(data: &mut [u8; PAGE_SIZE], offset: usize, value: u16) {
    data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_u32(data: &[u8; PAGE_SIZE], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn read_u64(data: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

pub(crate) fn write_u32(data: &mut [u8; PAGE_SIZE], offset: usize, value: u32) {
    data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(data: &mut [u8; PAGE_SIZE], offset: usize, value: u64) {
    data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn corrupt_page(message: impl Into<String>) -> common::DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use super::{
        PAGE_LSN_OFFSET, PAGE_TYPE_DATA, PAGE_TYPE_OFFSET, PAGE_VERSION, PAGE_VERSION_OFFSET,
        delete_row, init_page, insert_row, line_pointer, read_slot, set_page_lsn, set_tuple_header,
        validate, write_checksum,
    };
    use crate::codec::{decode_row, encode_row};
    use buffer::PageData;
    use common::{ColumnDef, DataType, INVALID_XID, TableSchema, Value, XMAX_COMMITTED};

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    id: 1,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn row() -> common::Row {
        common::Row {
            values: vec![Value::Integer(42), Value::Text("hi".to_string())],
        }
    }

    #[test]
    fn init_page_sets_page_format_version() {
        let mut data = PageData::default();
        init_page(&mut data.0, 7);

        assert_eq!(data.0[PAGE_VERSION_OFFSET], PAGE_VERSION);
    }

    #[test]
    fn validate_rejects_wrong_page_format_version() {
        let mut data = PageData::default();
        init_page(&mut data.0, 7);
        data.0[PAGE_VERSION_OFFSET] = PAGE_VERSION + 1;
        write_checksum(&mut data.0);

        let err = validate(&data.0).unwrap_err();
        assert!(err.message.contains("unsupported page version"));
    }

    #[test]
    fn validate_rejects_unversioned_legacy_page_header() {
        let mut data = PageData::default();
        data.0[PAGE_TYPE_OFFSET] = PAGE_TYPE_DATA;
        data.0[PAGE_VERSION_OFFSET] = 0;

        let err = validate(&data.0).unwrap_err();
        assert!(err.message.contains("unsupported page version"));
    }

    #[test]
    fn validate_rejects_v1_page_format() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        data.0[PAGE_VERSION_OFFSET] = 1;
        write_checksum(&mut data.0);

        let err = validate(&data.0).unwrap_err();
        assert!(err.message.contains("unsupported page version"));
    }

    #[test]
    fn set_page_lsn_round_trips_and_revalidates() {
        let mut data = PageData::default();
        init_page(&mut data.0, 3);
        set_page_lsn(&mut data.0, 0x0102_0304_0506_0708);

        // Checksum was refreshed, so the page still validates.
        validate(&data.0).unwrap();
        let stored = u64::from_le_bytes(
            data.0[PAGE_LSN_OFFSET..PAGE_LSN_OFFSET + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(stored, 0x0102_0304_0506_0708);
    }

    #[test]
    fn set_tuple_header_mutates_in_place_without_relocating() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let slot = insert_row(&mut data.0, &encode_row(&schema(), &row(), 7).unwrap()).unwrap();

        let before = read_slot(&data.0, slot);

        set_tuple_header(&mut data.0, slot, 99, (4, 5), XMAX_COMMITTED, 0x42).unwrap();

        // The tuple kept its exact offset and length: no relocation, no compaction.
        let after = read_slot(&data.0, slot);
        assert_eq!(after.offset, before.offset);
        assert_eq!(after.len, before.len);
        assert!(after.is_live());

        // The page checksum still verifies and the PageLSN was stamped.
        validate(&data.0).unwrap();
        assert_eq!(super::page_lsn(&data.0), 0x42);

        // The three header fields changed; xmin and the payload/null bitmap are intact.
        let bytes = super::read_row(&data.0, slot).unwrap().unwrap();
        let decoded = decode_row(&schema(), &bytes).unwrap();
        assert_eq!(decoded.xmax, 99);
        assert_eq!(decoded.t_ctid, (4, 5));
        assert_eq!(decoded.infomask, XMAX_COMMITTED);
        assert_eq!(decoded.xmin, 7);
        assert_eq!(decoded.row, row());
    }

    #[test]
    fn set_tuple_header_rejects_a_dead_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let slot = insert_row(&mut data.0, &encode_row(&schema(), &row(), 7).unwrap()).unwrap();
        assert!(delete_row(&mut data.0, slot).unwrap());

        // A tombstoned (DEAD) line pointer is not a valid mutation target.
        assert!(set_tuple_header(&mut data.0, slot, 1, (0, 0), 0, 1).is_err());
    }

    #[test]
    fn line_pointer_state_maps_live_to_normal_and_deleted_to_dead() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let slot = insert_row(&mut data.0, &encode_row(&schema(), &row(), 7).unwrap()).unwrap();

        // A freshly inserted slot is a NORMAL line pointer.
        assert_eq!(read_slot(&data.0, slot).flags, line_pointer::NORMAL);

        // Deleting through the existing path moves it to the DEAD state.
        assert!(delete_row(&mut data.0, slot).unwrap());
        assert_eq!(read_slot(&data.0, slot).flags, line_pointer::DEAD);
    }

    #[test]
    fn inserted_tuple_decodes_with_a_live_xmax() {
        // Sanity: the unmutated tuple is live (xmax invalid) before the primitive runs.
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let slot = insert_row(&mut data.0, &encode_row(&schema(), &row(), 7).unwrap()).unwrap();
        let bytes = super::read_row(&data.0, slot).unwrap().unwrap();
        assert_eq!(decode_row(&schema(), &bytes).unwrap().xmax, INVALID_XID);
    }
}
