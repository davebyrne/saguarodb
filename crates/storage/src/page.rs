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
    /// `insert_row` recycles the first `UNUSED` slot id before appending a fresh
    /// one (never a `DEAD` one — it may still have a dangling index entry).
    pub(super) const UNUSED: u16 = 0;
    /// Points at another slot **on the same page** (its target slot id is held in
    /// the line pointer's `offset` field). A HOT root slot whose original tuple has
    /// been pruned is replaced by a `REDIRECT` to the surviving root version, so an
    /// index entry referencing the stable root slot id still resolves (`mvcc.md`
    /// §5.2). H1 implements *reading* (resolving) a `REDIRECT`; H3 produces them.
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

/// The resolved state of a heap line pointer, returned by [`slot_state`] so the
/// engine's HOT read-side resolution (`mvcc.md` §5.2, Milestone H1) can branch on
/// it without reaching into the private [`Slot`] representation. `Redirect` carries
/// the target slot id (always on the same page, by the `REDIRECT` contract).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinePointer {
    /// Addresses a live tuple on this page (`read_row` returns its bytes).
    Normal,
    /// Tuple removed; the slot id is retained (index entries may still point here).
    Dead,
    /// Free for reuse; no tuple and no dangling index entry.
    Unused,
    /// Points at another slot on the **same page** (HOT root indirection); the
    /// payload is the target slot id.
    Redirect(u16),
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

/// The page's own id (stored in its header), used by HOT pruning to test whether a
/// tuple's `t_ctid` successor is on THIS page (a same-page HOT-chain member).
pub fn page_id(data: &[u8; PAGE_SIZE]) -> PageNum {
    read_u32(data, PAGE_ID_OFFSET)
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

/// Insert `row` into the page and return the slot id it landed in.
///
/// A free `UNUSED` slot id is reused before a fresh one is appended: the slot
/// array is scanned for the first `UNUSED` line pointer and, if one exists, that
/// slot id is rewritten to `(new_offset, len, NORMAL)`; otherwise a fresh slot id
/// is appended at `num_slots` (the historical behavior). Reuse bounds the slot
/// array under delete→vacuum→insert churn (`mvcc.md` §9 / Milestone F3b); without
/// it, every reclaimed line pointer would remain dead weight and the array would
/// grow unboundedly.
///
/// **Reuse `UNUSED` only, never `DEAD` — the F3b safety invariant.** A `DEAD` line
/// pointer may still have index entries pointing at it (index vacuum, F3a, has not
/// run for it yet); reusing it would let a stale index entry resolve to the *new*
/// tuple at that slot id — silent corruption. An `UNUSED` slot is guaranteed (by
/// the F2b→F3a→F3b VACUUM ordering: `reclaim_line_pointers` flips `DEAD → UNUSED`
/// only after `vacuum_indexes` removed every entry for the TID) to have no index
/// entry referencing it, so it is the only safe slot id to recycle. The scan is
/// O(slots-on-page) per insert; a free-space/free-slot map is the deferred
/// optimization (`mvcc.md` §12). Until VACUUM produces an `UNUSED` slot the scan
/// finds none and the append path runs exactly as before, so existing insert
/// behavior is unchanged.
pub fn insert_row(data: &mut [u8; PAGE_SIZE], row: &[u8]) -> Result<u16> {
    try_insert_row(data, row)?.ok_or_else(|| {
        DbError::storage(
            SqlState::InternalError,
            "page does not have enough free space",
        )
    })
}

/// Insert `row` into **this specific page** and return the slot id it landed in,
/// or `Ok(None)` when the page has no room (rather than erroring). This is the
/// page-local primitive the HOT-update fast path (`mvcc.md` §10 Milestone H2)
/// needs: a HOT update must place the new heap-only tuple on the SAME page as its
/// predecessor or fall back, so it must be able to ask "does it fit here?" and
/// proceed only if so. The general heap insert (`engine::write_new_row`) is free to
/// pick any page, which is NOT what HOT wants.
///
/// Slot selection is identical to [`insert_row`] (reuse the lowest `UNUSED` slot id
/// before appending a fresh one), so the slot a HOT insert lands in is reproduced
/// exactly by the `HeapInsert` redo (which re-runs [`insert_row`] and asserts the
/// same slot). `insert_row` is just this with the `None` mapped to a hard error.
pub fn try_insert_row(data: &mut [u8; PAGE_SIZE], row: &[u8]) -> Result<Option<u16>> {
    let header = validate(data)?;
    let row_len = u16::try_from(row.len())
        .map_err(|_| DbError::storage(SqlState::InternalError, "row is too large"))?;
    if free_bytes(header) < row.len() {
        return Ok(None);
    }

    let row_offset = header.free_start;
    let row_end = row_offset as usize + row.len();
    data[row_offset as usize..row_end].copy_from_slice(row);
    let new_slot = Slot {
        offset: row_offset,
        len: row_len,
        flags: line_pointer::NORMAL,
    };

    // Reuse the first UNUSED slot id if one exists (safety invariant above: ONLY
    // UNUSED, never DEAD). Reuse does not grow the slot array, so `free_bytes`
    // above (which is computed from `num_slots`) stays a valid lower bound. When no
    // UNUSED slot exists the append path runs, identical to the pre-F3b behavior.
    let slot_num = match first_unused_slot(data, header.num_slots) {
        Some(reused) => {
            write_slot(data, reused, new_slot);
            reused
        }
        None => {
            let appended = header.num_slots;
            write_slot(data, appended, new_slot);
            write_u16(data, NUM_SLOTS_OFFSET, appended + 1);
            appended
        }
    };
    write_u16(data, FREE_SPACE_OFFSET, row_offset + row_len);
    write_checksum(data);
    Ok(Some(slot_num))
}

/// The lowest slot id in `0..num_slots` whose line pointer is `UNUSED` (free for
/// reuse), or `None` if every slot is `NORMAL`/`DEAD`. A `DEAD` slot is
/// deliberately skipped: it may still have a dangling index entry (see
/// `insert_row`'s safety invariant). O(num_slots) — the deferred free-slot map
/// would make this O(1).
fn first_unused_slot(data: &[u8; PAGE_SIZE], num_slots: u16) -> Option<u16> {
    (0..num_slots).find(|&slot_num| read_slot(data, slot_num).flags == line_pointer::UNUSED)
}

/// Classify the line pointer at `slot_num` (`mvcc.md` §5.2) without reading the
/// tuple bytes — the read-side primitive HOT resolution (Milestone H1) needs to
/// detect and follow a `REDIRECT` and to validate a redirect target is `NORMAL`.
/// An out-of-bounds slot is a misuse and returns a structured `DbError` (never a
/// panic), matching the sibling primitives.
pub fn slot_state(data: &[u8; PAGE_SIZE], slot_num: u16) -> Result<LinePointer> {
    let header = validate(data)?;
    if slot_num >= header.num_slots {
        return Err(corrupt_page("slot number is out of bounds"));
    }
    let slot = read_slot(data, slot_num);
    Ok(match slot.flags {
        line_pointer::NORMAL => LinePointer::Normal,
        line_pointer::DEAD => LinePointer::Dead,
        line_pointer::UNUSED => LinePointer::Unused,
        // A REDIRECT stores its (same-page) target slot id in the `offset` field.
        line_pointer::REDIRECT => LinePointer::Redirect(slot.offset),
        // `validate_layout` already rejects any other flag value, so this is
        // unreachable on a validated page; guard defensively rather than panic.
        _ => return Err(corrupt_page("slot has invalid flags")),
    })
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

    compact_survivors_and_stamp(data, header, lsn)
}

/// Compact a heap page's live tuples in place: relocate every `NORMAL` tuple's
/// bytes contiguously from `HEADER_LEN` upward, reclaiming the bytes freed by any
/// `DEAD`/`UNUSED`/`REDIRECT` line pointer (and any prior gaps), without touching
/// the slot-id array order/ids — only each survivor's line-pointer `offset` is
/// rewritten. The PageLSN is stamped with `lsn` and the checksum refreshed.
///
/// This is the byte-reclaim half of HOT pruning (`mvcc.md` §9 / Milestone H3):
/// the engine first rewrites line pointers (a HOT root → `REDIRECT`, a fully-dead
/// root → `DEAD`, a heap-only dead member → `UNUSED`, a non-HOT/aborted dead slot
/// → `DEAD`) and then calls this to reclaim the bytes those non-`NORMAL` slots no
/// longer name. Unlike [`prune_and_compact`] it marks nothing dead itself — it
/// takes the page's slot states as given and only relocates survivor bytes — so an
/// index-referenced slot's id stays stable (indexes address it) while its tuple
/// bytes may move. A `REDIRECT` slot's `offset` field holds its target slot id
/// (not a byte offset) and is left untouched; only `NORMAL` survivors are
/// relocated.
#[allow(dead_code, reason = "consumed by VACUUM HOT pruning in H3")]
pub fn compact(data: &mut [u8; PAGE_SIZE], lsn: Lsn) -> Result<()> {
    let header = validate(data)?;
    compact_survivors_and_stamp(data, header, lsn)
}

/// Relocate every `NORMAL` survivor's bytes contiguously from `HEADER_LEN`,
/// rewriting each survivor's line-pointer `offset`, recompute `free_start`, stamp
/// `lsn`, and revalidate. Shared by [`prune_and_compact`] (after it marks the dead
/// slots) and [`compact`] (which marks nothing). Survivors are copied through a
/// scratch buffer first so overlapping source/destination ranges never corrupt a
/// tuple regardless of the survivors' original on-page order.
fn compact_survivors_and_stamp(
    data: &mut [u8; PAGE_SIZE],
    header: PageHeader,
    lsn: Lsn,
) -> Result<()> {
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
/// A slot reclaimed here becomes reusable: `insert_row` recycles the first
/// `UNUSED` slot id before appending a fresh one, which bounds the slot array
/// under delete→vacuum→insert churn. Reuse is safe precisely because this reclaim
/// runs only after index vacuum (F3a) has removed every entry for the TID, so an
/// `UNUSED` slot has no dangling index entry (see `insert_row`'s invariant).
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

/// Overwrite the line pointer at `slot_num` with a `REDIRECT` to `target_slot`
/// (on the same page) and refresh the checksum — the HOT-prune result that keeps a
/// stable, indexed root slot resolving to the surviving live tail after its
/// original tuple bytes are reclaimed (`mvcc.md` §5.2 / Milestone H3). The target
/// is stored in the line pointer's `offset` field; `len`/`flags` are overwritten.
///
/// Both ids must be in-bounds. This does **not** validate that the target is
/// currently `NORMAL` — the engine guarantees it (the redirect target is the first
/// not-dead-to-all chain member, always a live `NORMAL` tuple), and the read-side
/// resolver re-checks NORMAL on every resolution (`resolve_visible_in_chain`); a
/// test may also build a deliberately corrupt redirect-to-redirect to exercise
/// that guard. Does NOT stamp the PageLSN (a HOT prune stamps it once, after all
/// the page's line-pointer rewrites + the [`compact`] that follows).
#[allow(dead_code, reason = "consumed by VACUUM HOT pruning in H3")]
pub fn set_redirect(data: &mut [u8; PAGE_SIZE], slot_num: u16, target_slot: u16) -> Result<()> {
    let header = validate(data)?;
    if slot_num >= header.num_slots || target_slot >= header.num_slots {
        return Err(corrupt_page("slot number is out of bounds"));
    }
    write_slot(
        data,
        slot_num,
        Slot {
            offset: target_slot,
            len: 0,
            flags: line_pointer::REDIRECT,
        },
    );
    write_checksum(data);
    Ok(())
}

/// Flip the listed line pointers directly to `UNUSED`, freeing their slot ids for
/// reuse without the `DEAD` intermediate state. This is the HOT-prune primitive
/// (`mvcc.md` §9 / Milestone H3) for reclaiming a chain's **heap-only** members:
/// a `HEAP_ONLY` tuple has **no index entry of its own** (the H1/H2 invariant), so
/// — unlike a non-HOT or root slot — there is no dangling index entry to strip
/// first, and the slot is safe to free straight to `UNUSED` (the key HOT win, no
/// index vacuum needed). Each slot must currently be `NORMAL`; a non-`NORMAL` slot
/// (already `DEAD`/`UNUSED`/`REDIRECT`, or out of bounds) is a misuse and returns a
/// structured `DbError`. Does NOT stamp the PageLSN (the HOT prune stamps it once,
/// via the trailing [`compact`]); the freed tuple bytes are reclaimed by that
/// compaction. The checksum is refreshed so the page stays valid for the next
/// rewrite.
#[allow(dead_code, reason = "consumed by VACUUM HOT pruning in H3")]
pub fn free_slots_to_unused(data: &mut [u8; PAGE_SIZE], slots: &[u16]) -> Result<()> {
    let header = validate(data)?;
    for &slot_num in slots {
        if slot_num >= header.num_slots {
            return Err(corrupt_page("slot number is out of bounds"));
        }
        let mut slot = read_slot(data, slot_num);
        if slot.flags != line_pointer::NORMAL {
            return Err(DbError::storage(
                SqlState::InternalError,
                "cannot free a non-NORMAL slot directly to UNUSED",
            ));
        }
        slot.flags = line_pointer::UNUSED;
        write_slot(data, slot_num, slot);
    }
    write_checksum(data);
    Ok(())
}

/// Flip the listed line pointers to `DEAD` (retaining their slot ids so an index
/// entry still pointing here resolves to "no version" until index vacuum removes
/// it). The HOT-prune counterpart of [`prune_and_compact`]'s dead-marking, split
/// out so the engine can mark dead the index-referenced root of a fully-dead chain
/// — whether that root is currently `NORMAL` (a non-HOT/aborted dead tuple, or a
/// HOT root whose whole chain died) or `REDIRECT` (a previously-collapsed HOT root
/// whose surviving tail has since died) — and then run a single [`compact`] over
/// the whole page. Only `NORMAL`/`REDIRECT` slots may be marked: both are
/// index-referenced, so marking them `DEAD` schedules their entries for index
/// vacuum (F3a) then line-pointer reclaim (F3b). An already-`DEAD`/`UNUSED` or
/// out-of-bounds slot is a misuse and returns a structured `DbError`. Does NOT
/// stamp the PageLSN (the HOT prune stamps it once, via the trailing [`compact`]);
/// the checksum is refreshed so the page stays valid for the next rewrite.
#[allow(dead_code, reason = "consumed by VACUUM HOT pruning in H3")]
pub fn mark_slots_dead(data: &mut [u8; PAGE_SIZE], slots: &[u16]) -> Result<()> {
    let header = validate(data)?;
    for &slot_num in slots {
        if slot_num >= header.num_slots {
            return Err(corrupt_page("slot number is out of bounds"));
        }
        let mut slot = read_slot(data, slot_num);
        if slot.flags != line_pointer::NORMAL && slot.flags != line_pointer::REDIRECT {
            return Err(DbError::storage(
                SqlState::InternalError,
                "cannot mark a non-NORMAL/REDIRECT slot DEAD",
            ));
        }
        slot.flags = line_pointer::DEAD;
        write_slot(data, slot_num, slot);
    }
    write_checksum(data);
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
        // line-pointer reclaim (VACUUM, Milestone F); REDIRECT by HOT pruning
        // (Milestone H). Any other flag value is corrupt.
        if slot.flags != line_pointer::NORMAL
            && slot.flags != line_pointer::DEAD
            && slot.flags != line_pointer::UNUSED
            && slot.flags != line_pointer::REDIRECT
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
        // A REDIRECT's `offset` field is not a byte offset but a same-page target
        // slot id (`mvcc.md` §5.2); it must reference an in-bounds slot. (Whether
        // the target is itself `NORMAL` is enforced by the resolver, not here —
        // `validate` runs on every page read and must stay cheap/local.)
        if slot.flags == line_pointer::REDIRECT && slot.offset >= header.num_slots {
            return Err(corrupt_page("redirect target slot is out of bounds"));
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
        FREE_SPACE_OFFSET, HEADER_LEN, LinePointer, NUM_SLOTS_OFFSET, PAGE_LSN_OFFSET,
        PAGE_TYPE_DATA, PAGE_TYPE_OFFSET, PAGE_VERSION, PAGE_VERSION_OFFSET, compact, delete_row,
        free_slots_to_unused, init_page, insert_row, line_pointer, mark_slots_dead,
        prune_and_compact, read_row, read_slot, read_u16, reclaim_line_pointers, set_page_lsn,
        set_redirect, set_tuple_header, slot_state, validate, write_checksum, write_slot,
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
                    max_length: None,
                    default: None,
                },
                ColumnDef {
                    id: 1,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
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
    fn insert_row_reuses_an_unused_slot_without_growing_the_array() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x10, 10);
        let b = insert_blob(&mut data, 0x20, 12); // will be freed to UNUSED
        let c = insert_blob(&mut data, 0x30, 8);
        let slots_before = read_u16(&data.0, NUM_SLOTS_OFFSET);

        // Free slot `b` to UNUSED via the VACUUM primitives (prune -> DEAD, then
        // reclaim -> UNUSED), the only path that produces a reusable slot.
        prune_and_compact(&mut data.0, &[b], 1).unwrap();
        reclaim_line_pointers(&mut data.0, &[b], 2).unwrap();
        assert_eq!(read_slot(&data.0, b).flags, line_pointer::UNUSED);

        // A new insert recycles the UNUSED slot id `b` rather than appending a new
        // one: the slot count is unchanged and the new row reads back at slot `b`.
        let reused = insert_row(&mut data.0, &blob(0x40, 15)).unwrap();
        assert_eq!(reused, b, "insert reused the freed UNUSED slot id");
        assert_eq!(
            read_u16(&data.0, NUM_SLOTS_OFFSET),
            slots_before,
            "reusing a slot must not grow the slot array"
        );
        assert_eq!(read_row(&data.0, reused).unwrap(), Some(blob(0x40, 15)));
        assert_eq!(read_slot(&data.0, reused).flags, line_pointer::NORMAL);
        // The untouched neighbours still read their own bytes.
        assert_eq!(read_row(&data.0, a).unwrap(), Some(blob(0x10, 10)));
        assert_eq!(read_row(&data.0, c).unwrap(), Some(blob(0x30, 8)));
        validate(&data.0).unwrap();
    }

    #[test]
    fn insert_row_picks_the_lowest_unused_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x11, 6);
        let b = insert_blob(&mut data, 0x22, 6);
        let c = insert_blob(&mut data, 0x33, 6);

        // Free both `a` and `c` to UNUSED; the lower id (`a`) must be reused first.
        prune_and_compact(&mut data.0, &[a, c], 1).unwrap();
        reclaim_line_pointers(&mut data.0, &[a, c], 2).unwrap();

        let first = insert_row(&mut data.0, &blob(0x44, 6)).unwrap();
        assert_eq!(first, a, "the lowest UNUSED slot id is reused first");
        let second = insert_row(&mut data.0, &blob(0x55, 6)).unwrap();
        assert_eq!(second, c, "the next UNUSED slot id is reused next");
        let _ = b;
        validate(&data.0).unwrap();
    }

    #[test]
    fn insert_row_never_reuses_a_dead_slot() {
        // THE safety invariant: a DEAD slot (vacuum_heap ran, but the line pointer
        // was NOT reclaimed to UNUSED) may still have a dangling index entry, so
        // `insert_row` must never recycle it. A new insert appends a fresh slot id.
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0xA1, 10); // survivor
        let dead = insert_blob(&mut data, 0xD1, 12); // pruned to DEAD, NOT reclaimed
        prune_and_compact(&mut data.0, &[dead], 1).unwrap();
        assert_eq!(read_slot(&data.0, dead).flags, line_pointer::DEAD);
        let slots_before = read_u16(&data.0, NUM_SLOTS_OFFSET);

        let produced = insert_row(&mut data.0, &blob(0xE1, 9)).unwrap();
        assert_ne!(produced, dead, "a DEAD slot id must NEVER be reused");
        assert_eq!(
            read_u16(&data.0, NUM_SLOTS_OFFSET),
            slots_before + 1,
            "with no UNUSED slot, insert appends a fresh slot id"
        );
        // The DEAD slot is still DEAD (untouched) and reads as absent.
        assert_eq!(read_slot(&data.0, dead).flags, line_pointer::DEAD);
        assert_eq!(read_row(&data.0, dead).unwrap(), None);
        // The new row landed at the freshly appended slot, and the survivor is intact.
        assert_eq!(read_row(&data.0, produced).unwrap(), Some(blob(0xE1, 9)));
        assert_eq!(read_row(&data.0, a).unwrap(), Some(blob(0xA1, 10)));
        validate(&data.0).unwrap();
    }

    #[test]
    fn insert_row_appends_when_no_unused_slot_exists() {
        // The normal, no-vacuum-yet path: every slot is NORMAL, so insert appends a
        // fresh slot id at `num_slots`, exactly as before F3b.
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x01, 5);
        let b = insert_blob(&mut data, 0x02, 5);
        assert_eq!(a, 0);
        assert_eq!(b, 1);
        let c = insert_row(&mut data.0, &blob(0x03, 5)).unwrap();
        assert_eq!(c, 2, "with no UNUSED slot, ids are assigned sequentially");
        assert_eq!(read_u16(&data.0, NUM_SLOTS_OFFSET), 3);
        validate(&data.0).unwrap();
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

    // --- H1: REDIRECT line pointers + slot_state read-side accessor ---

    #[test]
    fn slot_state_classifies_normal_dead_and_unused() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let normal = insert_blob(&mut data, 0xA0, 8);
        let dead = insert_blob(&mut data, 0xB0, 8);
        let unused = insert_blob(&mut data, 0xC0, 8);
        // Drive `dead` to DEAD and `unused` to UNUSED via the VACUUM primitives.
        prune_and_compact(&mut data.0, &[dead, unused], 1).unwrap();
        reclaim_line_pointers(&mut data.0, &[unused], 2).unwrap();

        assert_eq!(slot_state(&data.0, normal).unwrap(), LinePointer::Normal);
        assert_eq!(slot_state(&data.0, dead).unwrap(), LinePointer::Dead);
        assert_eq!(slot_state(&data.0, unused).unwrap(), LinePointer::Unused);
    }

    #[test]
    fn slot_state_reports_redirect_target() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let root = insert_blob(&mut data, 0x11, 8);
        let target = insert_blob(&mut data, 0x22, 8);
        set_redirect(&mut data.0, root, target).unwrap();

        assert_eq!(
            slot_state(&data.0, root).unwrap(),
            LinePointer::Redirect(target)
        );
        // The target itself is untouched and still NORMAL.
        assert_eq!(slot_state(&data.0, target).unwrap(), LinePointer::Normal);
        // A redirect slot reads no tuple bytes directly.
        assert_eq!(read_row(&data.0, root).unwrap(), None);
    }

    #[test]
    fn slot_state_rejects_out_of_bounds_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let _ = insert_blob(&mut data, 0x33, 8);
        assert!(slot_state(&data.0, 99).is_err());
    }

    #[test]
    fn validate_accepts_an_in_bounds_redirect() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let root = insert_blob(&mut data, 0x44, 8);
        let target = insert_blob(&mut data, 0x55, 8);
        set_redirect(&mut data.0, root, target).unwrap();

        // A page carrying a NORMAL target + a REDIRECT to it is valid.
        validate(&data.0).unwrap();
    }

    #[test]
    fn validate_rejects_a_redirect_to_an_out_of_bounds_slot() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let root = insert_blob(&mut data, 0x66, 8);
        let _target = insert_blob(&mut data, 0x77, 8);
        // Hand-write a REDIRECT whose target slot id is past the slot array.
        let num_slots = read_u16(&data.0, NUM_SLOTS_OFFSET);
        write_slot(
            &mut data.0,
            root,
            super::Slot {
                offset: num_slots, // out of bounds
                len: 0,
                flags: line_pointer::REDIRECT,
            },
        );
        write_checksum(&mut data.0);
        assert!(validate(&data.0).is_err());
    }

    // --- H3: HOT-prune line-pointer rewrites + compaction ---

    #[test]
    fn compact_reclaims_bytes_of_non_normal_slots_keeping_ids_stable() {
        // A page with a NORMAL survivor, a slot redirected away, a slot freed to
        // UNUSED, and a slot marked DEAD. `compact` repacks ONLY the NORMAL survivors,
        // reclaims the others' bytes, and keeps every slot id stable.
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let keep = insert_blob(&mut data, 0xA0, 10);
        let redirected = insert_blob(&mut data, 0xB0, 30); // its bytes will be reclaimed
        let heap_only = insert_blob(&mut data, 0xC0, 20); // freed straight to UNUSED
        let dead_root = insert_blob(&mut data, 0xD0, 15); // marked DEAD
        let tail = insert_blob(&mut data, 0xE0, 8); // NORMAL survivor (redirect target)
        let free_before = read_u16(&data.0, FREE_SPACE_OFFSET);

        free_slots_to_unused(&mut data.0, &[heap_only]).unwrap();
        set_redirect(&mut data.0, redirected, tail).unwrap();
        mark_slots_dead(&mut data.0, &[dead_root]).unwrap();
        compact(&mut data.0, 0xFEED).unwrap();

        // Slot ids stayed stable; states are as set; the NORMAL survivors keep bytes.
        assert_eq!(slot_state(&data.0, keep).unwrap(), LinePointer::Normal);
        assert_eq!(read_row(&data.0, keep).unwrap(), Some(blob(0xA0, 10)));
        assert_eq!(
            slot_state(&data.0, redirected).unwrap(),
            LinePointer::Redirect(tail)
        );
        assert_eq!(slot_state(&data.0, heap_only).unwrap(), LinePointer::Unused);
        assert_eq!(slot_state(&data.0, dead_root).unwrap(), LinePointer::Dead);
        assert_eq!(slot_state(&data.0, tail).unwrap(), LinePointer::Normal);
        assert_eq!(read_row(&data.0, tail).unwrap(), Some(blob(0xE0, 8)));

        // Only the two NORMAL survivors (keep=10 + tail=8) remain in the live region;
        // the redirected(30)/heap_only(20)/dead(15) bytes were all reclaimed.
        validate(&data.0).unwrap();
        assert_eq!(super::page_lsn(&data.0), 0xFEED);
        let free_after = read_u16(&data.0, FREE_SPACE_OFFSET);
        assert_eq!(free_after as usize, HEADER_LEN + 10 + 8);
        assert!(free_after < free_before);
    }

    #[test]
    fn free_slots_to_unused_rejects_non_normal_slots() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let a = insert_blob(&mut data, 0x11, 8);
        assert!(delete_row(&mut data.0, a).unwrap()); // now DEAD
        // A DEAD/UNUSED/REDIRECT slot is not a valid NORMAL target.
        assert!(free_slots_to_unused(&mut data.0, &[a]).is_err());
        assert!(free_slots_to_unused(&mut data.0, &[99]).is_err());
    }

    #[test]
    fn mark_slots_dead_accepts_normal_and_redirect_but_not_dead() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let normal = insert_blob(&mut data, 0x22, 8);
        let redirect_root = insert_blob(&mut data, 0x33, 8);
        let target = insert_blob(&mut data, 0x44, 8);
        set_redirect(&mut data.0, redirect_root, target).unwrap();

        // A NORMAL root and a REDIRECT root can both be marked DEAD (both index-ref'd).
        mark_slots_dead(&mut data.0, &[normal, redirect_root]).unwrap();
        assert_eq!(slot_state(&data.0, normal).unwrap(), LinePointer::Dead);
        assert_eq!(
            slot_state(&data.0, redirect_root).unwrap(),
            LinePointer::Dead
        );
        validate(&data.0).unwrap();

        // A slot already DEAD is not a valid target.
        assert!(mark_slots_dead(&mut data.0, &[normal]).is_err());
    }

    #[test]
    fn set_redirect_then_resolve_reads_no_bytes_at_the_root() {
        let mut data = PageData::default();
        init_page(&mut data.0, 1);
        let root = insert_blob(&mut data, 0x55, 8);
        let target = insert_blob(&mut data, 0x66, 8);
        set_redirect(&mut data.0, root, target).unwrap();
        assert_eq!(
            slot_state(&data.0, root).unwrap(),
            LinePointer::Redirect(target)
        );
        assert_eq!(read_row(&data.0, root).unwrap(), None);
        // Redirect to an out-of-bounds slot is rejected.
        let n = read_u16(&data.0, NUM_SLOTS_OFFSET);
        assert!(set_redirect(&mut data.0, root, n).is_err());
        validate(&data.0).unwrap();
    }
}
