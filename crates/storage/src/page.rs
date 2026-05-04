use buffer::PAGE_SIZE;
use common::{DbError, PageNum, Result, SqlState};

pub const PAGE_TYPE_DATA: u8 = 1;

const HEADER_LEN: usize = 13;
const PAGE_ID_OFFSET: usize = 0;
const PAGE_TYPE_OFFSET: usize = 4;
const NUM_SLOTS_OFFSET: usize = 5;
const FREE_SPACE_OFFSET: usize = 7;
const CHECKSUM_OFFSET: usize = 9;
const SLOT_LEN: usize = 6;
const SLOT_DEAD: u16 = 1;
const SLOT_LIVE: u16 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHeader {
    page_id: PageNum,
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
    fn is_live(self) -> bool {
        self.flags == SLOT_LIVE
    }
}

pub fn init_page(data: &mut [u8; PAGE_SIZE], page_id: PageNum) {
    data.fill(0);
    write_u32(data, PAGE_ID_OFFSET, page_id);
    data[PAGE_TYPE_OFFSET] = PAGE_TYPE_DATA;
    write_u16(data, NUM_SLOTS_OFFSET, 0);
    write_u16(data, FREE_SPACE_OFFSET, HEADER_LEN as u16);
    write_checksum(data);
}

pub fn validate(data: &[u8; PAGE_SIZE]) -> Result<PageHeader> {
    if data[PAGE_TYPE_OFFSET] != PAGE_TYPE_DATA {
        return Err(corrupt_page("unexpected page type"));
    }
    let stored_checksum = read_u32(data, CHECKSUM_OFFSET);
    if stored_checksum != checksum(data) {
        return Err(corrupt_page("page checksum mismatch"));
    }

    let header = PageHeader {
        page_id: read_u32(data, PAGE_ID_OFFSET),
        num_slots: read_u16(data, NUM_SLOTS_OFFSET),
        free_start: read_u16(data, FREE_SPACE_OFFSET),
    };
    validate_layout(data, header)?;
    Ok(header)
}

pub fn is_initialized(data: &[u8; PAGE_SIZE]) -> bool {
    data[PAGE_TYPE_OFFSET] == PAGE_TYPE_DATA
}

pub fn page_id(data: &[u8; PAGE_SIZE]) -> Result<PageNum> {
    Ok(validate(data)?.page_id)
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
            flags: SLOT_LIVE,
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
    slot.flags = SLOT_DEAD;
    write_slot(data, slot_num, slot);
    write_checksum(data);
    Ok(true)
}

pub fn live_rows(data: &[u8; PAGE_SIZE]) -> Result<Vec<(u16, Vec<u8>)>> {
    let header = validate(data)?;
    let mut rows = Vec::new();
    for slot_num in 0..header.num_slots {
        if let Some(row) = read_row(data, slot_num)? {
            rows.push((slot_num, row));
        }
    }
    Ok(rows)
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
        if slot.flags != SLOT_LIVE && slot.flags != SLOT_DEAD {
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

fn write_checksum(data: &mut [u8; PAGE_SIZE]) {
    write_u32(data, CHECKSUM_OFFSET, checksum(data));
}

fn read_u16(data: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

fn write_u16(data: &mut [u8; PAGE_SIZE], offset: usize, value: u16) {
    data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn read_u32(data: &[u8; PAGE_SIZE], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn write_u32(data: &mut [u8; PAGE_SIZE], offset: usize, value: u32) {
    data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn corrupt_page(message: impl Into<String>) -> common::DbError {
    DbError::storage(SqlState::InternalError, message)
}
