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
    /// Free for reuse. Produced by `reclaim_line_pointers` (VACUUM, Milestone F);
    /// `insert_row` does not yet reuse an `UNUSED` slot id (it always appends).
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

/// Prune the listed dead slots and compact the page's live tuples in a single
/// pass (the intra-page heap-prune primitive, `mvcc.md` §9 / Milestone F2).
///
/// `dead_slots` are line pointers the caller (F2b) has classified as
/// dead-to-everyone via `is_dead_to_all` — this function does **not** classify;
/// it only rewrites the page. For each:
///
/// - Each `dead_slot` is flipped `NORMAL -> DEAD`. The slot id is **retained**
///   (index entries may still reference it); reclaiming the line pointer to
///   `UNUSED` is a later step (`reclaim_line_pointers`, owned by F3b).
/// - The surviving `NORMAL` tuples are relocated so their bytes are contiguous
///   from `HEADER_LEN` upward, reclaiming the bytes freed by the now-`DEAD`
///   slots and any prior gaps. Each survivor's line-pointer **`offset` is
///   rewritten** to its new location; the slot-id array order/ids and every
///   survivor's `len` are unchanged, so `read_row(data, slot)` returns the
///   identical bytes for the same slot id after compaction. `free_start` is
///   recomputed for the compacted layout.
/// - The PageLSN is stamped with `lsn` and the checksum refreshed (via
///   `set_page_lsn`, exactly like `set_tuple_header`), so the checksum covers
///   the compacted bytes.
///
/// Survivors are copied through a scratch buffer before being written back, so
/// overlapping source/destination ranges never corrupt a tuple regardless of
/// the survivors' original order on the page. A `dead_slot` that is not a live
/// `NORMAL` line pointer (already `DEAD`/`UNUSED`, or out of bounds) is a misuse
/// and returns a structured `DbError` rather than silently skipping.
#[allow(dead_code, reason = "consumed by VACUUM in F2b/F3b")]
pub fn prune_and_compact(data: &mut [u8; PAGE_SIZE], dead_slots: &[u16], lsn: Lsn) -> Result<()> {
    let header = validate(data)?;

    // Mark the listed slots DEAD first, validating each is a live target.
    for &slot_num in dead_slots {
        if slot_num >= header.num_slots {
            return Err(corrupt_page("slot number is out of bounds"));
        }
        let mut slot = read_slot(data, slot_num);
        if !slot.is_live() {
            return Err(DbError::storage(
                SqlState::InternalError,
                "cannot prune a non-live slot",
            ));
        }
        slot.flags = line_pointer::DEAD;
        write_slot(data, slot_num, slot);
    }

    // Snapshot every surviving NORMAL tuple's bytes into a scratch buffer so the
    // copy-back never reads a region a prior survivor has already overwritten.
    let mut survivors: Vec<(u16, Vec<u8>)> = Vec::new();
    for slot_num in 0..header.num_slots {
        let slot = read_slot(data, slot_num);
        if slot.is_live() {
            let start = slot.offset as usize;
            let end = start + slot.len as usize;
            survivors.push((slot_num, data[start..end].to_vec()));
        }
    }

    // Lay survivors back down contiguously from HEADER_LEN, rewriting offsets.
    let mut cursor = HEADER_LEN;
    for (slot_num, bytes) in &survivors {
        let new_offset = cursor;
        let new_end = new_offset + bytes.len();
        data[new_offset..new_end].copy_from_slice(bytes);
        let mut slot = read_slot(data, *slot_num);
        slot.offset =
            u16::try_from(new_offset).map_err(|_| corrupt_page("compacted offset overflows"))?;
        write_slot(data, *slot_num, slot);
        cursor = new_end;
    }

    write_u16(
        data,
        FREE_SPACE_OFFSET,
        u16::try_from(cursor).map_err(|_| corrupt_page("compacted free_start overflows"))?,
    );
    set_page_lsn(data, lsn);

    // Re-derive and revalidate the compacted layout (covers checksum + offsets).
    validate(data)?;
    Ok(())
}

/// Reclaim the listed `DEAD` line pointers to `UNUSED`, making their slot ids
/// reusable by a future `insert_row` (the line-pointer reclaim primitive,
/// `mvcc.md` §9 / Milestone F3b). Each slot must currently be `DEAD`; a
/// non-`DEAD` slot (still `NORMAL`/already `UNUSED`, or out of bounds) is a
/// misuse and returns a structured `DbError`. The PageLSN is stamped with `lsn`
/// and the checksum refreshed via `set_page_lsn`.
///
/// Note `insert_row` currently always **appends** a fresh slot id (it never
/// scans for a reusable `UNUSED` slot), so flipping `DEAD -> UNUSED` reclaims no
/// space today — it is correct and forward-looking; slot-id reuse on insert is a
/// separate, later change and is intentionally not added here.
#[allow(dead_code, reason = "consumed by VACUUM in F2b/F3b")]
pub fn reclaim_line_pointers(data: &mut [u8; PAGE_SIZE], slots: &[u16], lsn: Lsn) -> Result<()> {
    let header = validate(data)?;
    for &slot_num in slots {
        if slot_num >= header.num_slots {
            return Err(corrupt_page("slot number is out of bounds"));
        }
        let mut slot = read_slot(data, slot_num);
        if slot.flags != line_pointer::DEAD {
            return Err(DbError::storage(
                SqlState::InternalError,
                "cannot reclaim a slot that is not DEAD",
            ));
        }
        slot.flags = line_pointer::UNUSED;
        write_slot(data, slot_num, slot);
    }
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
        // NORMAL/DEAD are produced by inserts/deletes; UNUSED is produced by
        // line-pointer reclaim (VACUUM, Milestone F). REDIRECT (HOT, Milestone H)
        // is reserved and not yet written, so a page carrying it — or any other
        // flag value — is corrupt.
        if slot.flags != line_pointer::NORMAL
            && slot.flags != line_pointer::DEAD
            && slot.flags != line_pointer::UNUSED
        {
            return Err(corrupt_page("slot has invalid flags"));
        }
        // Only NORMAL line pointers name live bytes, so only they must lie within
        // the live region. After compaction/reclaim a DEAD or UNUSED slot's
        // `(offset, len)` no longer addresses live data and is left unconstrained.
        if slot.flags == line_pointer::NORMAL {
            let start = slot.offset as usize;
            let end = start
                .checked_add(slot.len as usize)
                .ok_or_else(|| corrupt_page("slot length overflows"))?;
            if start < HEADER_LEN || end > header.free_start as usize {
                return Err(corrupt_page("slot points outside row region"));
            }
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
        FREE_SPACE_OFFSET, HEADER_LEN, PAGE_LSN_OFFSET, PAGE_TYPE_DATA, PAGE_TYPE_OFFSET,
        PAGE_VERSION, PAGE_VERSION_OFFSET, delete_row, init_page, insert_row, line_pointer,
        prune_and_compact, read_row, read_slot, read_u16, reclaim_line_pointers, set_page_lsn,
        set_tuple_header, validate, write_checksum, write_slot,
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

    // --- F2a: prune_and_compact / reclaim_line_pointers / validate_layout ---

    /// A page-level tuple is an opaque byte payload as far as compaction cares,
    /// so these tests insert distinct-byte blobs of varied length. Using a unique
    /// fill byte per slot proves a survivor's bytes belong to its own slot id after
    /// relocation (not a neighbour's), which an encoded-row helper would obscure.
    fn blob(fill: u8, len: usize) -> Vec<u8> {
        vec![fill; len]
    }

    fn insert_blob(data: &mut PageData, fill: u8, len: usize) -> u16 {
        insert_row(&mut data.0, &blob(fill, len)).unwrap()
    }

    #[test]
    fn prune_and_compact_relocates_survivors_and_frees_dead_bytes() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        // Varied sizes; record each slot's id, fill byte, and length.
        let specs: [(u8, usize); 5] = [(0xA1, 10), (0xB2, 30), (0xC3, 5), (0xD4, 20), (0xE5, 15)];
        let slots: Vec<u16> = specs
            .iter()
            .map(|&(fill, len)| insert_blob(&mut data, fill, len))
            .collect();
        let free_before = read_u16(&data.0, FREE_SPACE_OFFSET);

        // Dead slots interleaved among survivors (indices 1 and 3 in insert order).
        let dead = [slots[1], slots[3]];
        let dead_bytes: usize = specs[1].1 + specs[3].1;

        prune_and_compact(&mut data.0, &dead, 0xFEED).unwrap();

        // Survivors readable by their ORIGINAL slot id with IDENTICAL bytes.
        for (i, &(fill, len)) in specs.iter().enumerate() {
            let got = read_row(&data.0, slots[i]).unwrap();
            if dead.contains(&slots[i]) {
                assert_eq!(got, None, "dead slot {i} must read None");
                assert_eq!(read_slot(&data.0, slots[i]).flags, line_pointer::DEAD);
            } else {
                assert_eq!(
                    got,
                    Some(blob(fill, len)),
                    "survivor {i} bytes/len preserved"
                );
            }
        }

        // Checksum verifies and the PageLSN was stamped.
        validate(&data.0).unwrap();
        assert_eq!(super::page_lsn(&data.0), 0xFEED);

        // free_start moved down by exactly the dead tuples' total size (no gaps
        // before either dead slot in this layout, so compaction reclaims exactly
        // those bytes).
        let free_after = read_u16(&data.0, FREE_SPACE_OFFSET);
        assert_eq!(free_before as usize - free_after as usize, dead_bytes);

        // Survivors are contiguous from HEADER_LEN, in stable slot-id order.
        let mut cursor = HEADER_LEN;
        for (i, _) in specs.iter().enumerate() {
            if dead.contains(&slots[i]) {
                continue;
            }
            let s = read_slot(&data.0, slots[i]);
            assert_eq!(
                s.offset as usize, cursor,
                "survivor {i} packed contiguously"
            );
            cursor += s.len as usize;
        }
        assert_eq!(cursor, free_after as usize);
    }

    #[test]
    fn prune_and_compact_all_slots_dead_yields_empty_valid_page() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x11, 12);
        let b = insert_blob(&mut data, 0x22, 7);

        prune_and_compact(&mut data.0, &[a, b], 9).unwrap();

        // No live tuples remain; both read None and the page still validates.
        assert_eq!(read_row(&data.0, a).unwrap(), None);
        assert_eq!(read_row(&data.0, b).unwrap(), None);
        validate(&data.0).unwrap();
        // free_start collapsed back to the header (no live bytes).
        assert_eq!(read_u16(&data.0, FREE_SPACE_OFFSET) as usize, HEADER_LEN);
    }

    #[test]
    fn prune_and_compact_no_dead_slots_is_a_lossless_noop() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x33, 9);
        let b = insert_blob(&mut data, 0x44, 25);
        let free_before = read_u16(&data.0, FREE_SPACE_OFFSET);

        prune_and_compact(&mut data.0, &[], 5).unwrap();

        assert_eq!(read_row(&data.0, a).unwrap(), Some(blob(0x33, 9)));
        assert_eq!(read_row(&data.0, b).unwrap(), Some(blob(0x44, 25)));
        // Already contiguous from the bottom, so nothing moved.
        assert_eq!(read_u16(&data.0, FREE_SPACE_OFFSET), free_before);
        validate(&data.0).unwrap();
    }

    #[test]
    fn prune_and_compact_single_survivor_relocates_to_header() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x55, 8); // becomes dead
        let b = insert_blob(&mut data, 0x66, 16); // survivor, starts above `a`

        prune_and_compact(&mut data.0, &[a], 1).unwrap();

        // The lone survivor slid down to HEADER_LEN; its bytes are intact.
        assert_eq!(read_row(&data.0, b).unwrap(), Some(blob(0x66, 16)));
        assert_eq!(read_slot(&data.0, b).offset as usize, HEADER_LEN);
        assert_eq!(
            read_u16(&data.0, FREE_SPACE_OFFSET) as usize,
            HEADER_LEN + 16
        );
        validate(&data.0).unwrap();
    }

    #[test]
    fn prune_and_compact_rejects_a_non_live_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x77, 10);
        assert!(delete_row(&mut data.0, a).unwrap());

        // Pruning an already-DEAD slot is a misuse, not a silent skip.
        assert!(prune_and_compact(&mut data.0, &[a], 1).is_err());
        // Out-of-bounds slot likewise errors.
        assert!(prune_and_compact(&mut data.0, &[99], 1).is_err());
    }

    #[test]
    fn reclaim_line_pointers_moves_dead_to_unused() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x88, 10);
        assert!(delete_row(&mut data.0, a).unwrap());
        assert_eq!(read_slot(&data.0, a).flags, line_pointer::DEAD);

        reclaim_line_pointers(&mut data.0, &[a], 0xABCD).unwrap();

        assert_eq!(read_slot(&data.0, a).flags, line_pointer::UNUSED);
        assert_eq!(read_row(&data.0, a).unwrap(), None);
        validate(&data.0).unwrap();
        assert_eq!(super::page_lsn(&data.0), 0xABCD);
    }

    #[test]
    fn reclaim_line_pointers_rejects_a_normal_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x99, 10); // still NORMAL

        assert!(reclaim_line_pointers(&mut data.0, &[a], 1).is_err());
        // And a slot that is already UNUSED is also not a valid DEAD target.
        assert!(delete_row(&mut data.0, a).unwrap());
        reclaim_line_pointers(&mut data.0, &[a], 1).unwrap();
        assert!(reclaim_line_pointers(&mut data.0, &[a], 1).is_err());
    }

    #[test]
    fn validate_accepts_normal_dead_and_unused_after_compaction() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0xA0, 10); // survivor (NORMAL)
        let b = insert_blob(&mut data, 0xB0, 12); // pruned -> DEAD
        let c = insert_blob(&mut data, 0xC0, 8); // pruned -> DEAD -> reclaimed UNUSED

        prune_and_compact(&mut data.0, &[b, c], 1).unwrap();
        reclaim_line_pointers(&mut data.0, &[c], 2).unwrap();

        // A page carrying NORMAL + DEAD + UNUSED slots is valid.
        validate(&data.0).unwrap();
        assert_eq!(read_slot(&data.0, a).flags, line_pointer::NORMAL);
        assert_eq!(read_slot(&data.0, b).flags, line_pointer::DEAD);
        assert_eq!(read_slot(&data.0, c).flags, line_pointer::UNUSED);
    }

    #[test]
    fn validate_still_rejects_a_corrupt_normal_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0xAA, 10);

        // Push the NORMAL slot's end past free_start (out of the live region).
        let free_start = read_u16(&data.0, FREE_SPACE_OFFSET);
        let mut slot = read_slot(&data.0, a);
        slot.offset = free_start - 4; // end = free_start - 4 + 10 > free_start
        write_slot(&mut data.0, a, slot);
        write_checksum(&mut data.0);
        assert!(validate(&data.0).is_err());

        // Reset, then corrupt with an out-of-bounds offset below the header.
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0xAB, 10);
        let mut slot = read_slot(&data.0, a);
        slot.offset = (HEADER_LEN - 1) as u16;
        write_slot(&mut data.0, a, slot);
        write_checksum(&mut data.0);
        assert!(validate(&data.0).is_err());
    }

    #[test]
    fn validate_rejects_an_unconstrained_offset_only_when_normal() {
        // A DEAD slot with a stale (out-of-region) offset is valid; flipping the
        // same slot back to NORMAL makes the identical offset corrupt.
        //
        // `b` survives at the bottom; `a` is pruned. `a` sits ABOVE `b` on the
        // page, so after `b` compacts down, `free_start` shrinks below `a`'s
        // (retained) stale offset — exactly the case the extension must tolerate
        // for DEAD but reject for NORMAL.
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let b = insert_blob(&mut data, 0xDD, 6); // survivor, near the bottom
        let a = insert_blob(&mut data, 0xCC, 40); // pruned; high stale offset
        prune_and_compact(&mut data.0, &[a], 1).unwrap(); // `a` is DEAD, offset stale
        validate(&data.0).unwrap();

        // The DEAD slot's offset still names its pre-compaction location, which is
        // now beyond the shrunken live region — unconstrained for DEAD, but as
        // NORMAL it must point into the live region.
        let mut dead = read_slot(&data.0, a);
        let live_end = read_u16(&data.0, FREE_SPACE_OFFSET);
        assert!(dead.offset as usize + dead.len as usize > live_end as usize);
        dead.flags = line_pointer::NORMAL;
        write_slot(&mut data.0, a, dead);
        write_checksum(&mut data.0);
        assert!(validate(&data.0).is_err());
        let _ = b;
    }
}
