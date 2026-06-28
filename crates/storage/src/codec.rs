use common::{
    DataType, DbError, FROZEN_XID, INVALID_XID, Key, PageNum, Result, Row, SqlState, TableSchema,
    TxnId, Value, XMIN_COMMITTED,
};

/// On-page row encoding version. The current MVCC layout (v2) is
/// `[version=2][infomask:2][xmin:8][xmax:8][t_ctid:6][null_bitmap][columns]`;
/// the legacy pre-MVCC layout (v1) is `[version=1][null_bitmap][columns]`.
/// `decode_row` reads both so pre-MVCC heaps keep decoding.
const ROW_FORMAT_VERSION: u8 = 2;

/// Legacy pre-MVCC row layout: `[version=1][null_bitmap][columns]` with no
/// per-version transaction header. Still decoded for backward compatibility.
const ROW_FORMAT_VERSION_V1: u8 = 1;

/// Byte width of the v2 MVCC header that precedes the v1-style null bitmap:
/// `infomask(2) + xmin(8) + xmax(8) + t_ctid(6)`. The version byte and null
/// bitmap are accounted for separately.
const V2_MVCC_HEADER_LEN: usize = 2 + 8 + 8 + 6;

/// Sentinel `t_ctid` meaning "no successor / this is the latest version". Used in
/// place of a literal self-pointer because the encoder does not know the slot a
/// tuple will land in; a real successor pointer is stamped later (Milestone B).
pub(crate) const INVALID_TID: (PageNum, u16) = (u32::MAX, u16::MAX);

/// `infomask` hint bits (bit positions in the v2 header's `u16`). The four
/// `*_COMMITTED`/`*_ABORTED` settled-status bits are owned by `common` (re-used by
/// the visibility predicate as a single source of truth) and re-exported here; the
/// two HEAP bits are storage-private and wired by HOT (read in H1, set in H2; see
/// below).
///
/// ```text
/// bit 0: XMIN_COMMITTED  bit 1: XMIN_ABORTED   (in common, used by is_visible)
/// bit 2: XMAX_COMMITTED  bit 3: XMAX_ABORTED   (in common, used by is_visible)
/// bit 4: HEAP_ONLY       bit 5: HOT_UPDATED    (storage-private, HOT — see below)
/// bits 6-15: reserved (must be 0)
/// ```
/// `XMIN_COMMITTED` (used by the v1-decode synthesized header) and the other three
/// settled-status bits come from [`common`]. HOT (Milestone H) wires the two HEAP
/// bits in: H1 *reads* them on the HOT-chain walk; H2 *sets* `HOT_UPDATED` on a
/// HOT-updated root and `HEAP_ONLY` on its heap-only successor.
///
/// A `HEAP_ONLY` tuple has **no index entry of its own**: it is reachable only by
/// walking `t_ctid` from its HOT-chain root (which IS indexed). The walk follows
/// `t_ctid` into a successor only when the current tuple is `HOT_UPDATED` and the
/// successor is `HEAP_ONLY` — staying strictly within one HOT-chain segment so a
/// single visible row is never reached via two index entries (`mvcc.md` §5.1, §10
/// Milestone H1).
pub(crate) const HEAP_ONLY: u16 = 1 << 4;
/// HOT: the tuple was HOT-updated in place; its `t_ctid` successor is `HEAP_ONLY`.
pub(crate) const HOT_UPDATED: u16 = 1 << 5;

/// A decoded row plus its MVCC tuple header. Later milestones (visibility,
/// versioning) read `xmin`/`xmax`/`t_ctid`/`infomask`; current callers that only
/// need the column values use `row`. v1 tuples synthesize a frozen, never-deleted
/// header so they are visible to every snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedRow {
    pub row: Row,
    pub xmin: TxnId,
    pub xmax: TxnId,
    pub t_ctid: (PageNum, u16),
    pub infomask: u16,
}

const KEY_TAG_NULL: u8 = 0;
const KEY_TAG_INTEGER: u8 = 1;
const KEY_TAG_TEXT: u8 = 2;
const KEY_TAG_BOOLEAN: u8 = 3;
const KEY_TAG_DATE: u8 = 4;
const KEY_TAG_TIMESTAMP: u8 = 5;

/// Encode a primary key into the self-describing byte form stored in B-tree
/// nodes: `[n: u16]` then each value as `[tag][payload]`. Self-describing so the
/// btree can decode and order keys without a schema. Not order-preserving — the
/// btree compares decoded `Key`s via `Ord`, matching the in-memory directory.
pub(crate) fn encode_key(key: &Key) -> Result<Vec<u8>> {
    let count = u16::try_from(key.0.len())
        .map_err(|_| DbError::storage(SqlState::InternalError, "key has too many columns"))?;
    let mut bytes = count.to_le_bytes().to_vec();
    for value in &key.0 {
        match value {
            Value::Null => bytes.push(KEY_TAG_NULL),
            Value::Integer(value) => {
                bytes.push(KEY_TAG_INTEGER);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Text(value) => {
                bytes.push(KEY_TAG_TEXT);
                let len = u32::try_from(value.len()).map_err(|_| {
                    DbError::storage(SqlState::InternalError, "key text is too large")
                })?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
            Value::Boolean(value) => {
                bytes.push(KEY_TAG_BOOLEAN);
                bytes.push(u8::from(*value));
            }
            Value::Date(value) => {
                bytes.push(KEY_TAG_DATE);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Timestamp(value) => {
                bytes.push(KEY_TAG_TIMESTAMP);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
    Ok(bytes)
}

pub(crate) fn decode_key(bytes: &[u8]) -> Result<Key> {
    let (key, consumed) = decode_key_prefix(bytes)?;
    if consumed != bytes.len() {
        return Err(corrupt_row("key has trailing bytes"));
    }
    Ok(key)
}

/// Decode the leading `Key` of `bytes` and report how many bytes it consumed,
/// leaving any trailing bytes (a `(key, value)` separator's value tiebreaker in
/// the multi-entry B-tree) for the caller. `decode_key` is this plus a
/// no-trailing-bytes check.
pub(crate) fn decode_key_prefix(bytes: &[u8]) -> Result<(Key, usize)> {
    let mut offset = 0;
    let count = u16::from_le_bytes(
        read_exact(bytes, &mut offset, 2)?
            .try_into()
            .expect("read_exact returns 2 bytes"),
    );
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tag = read_exact(bytes, &mut offset, 1)?[0];
        let value = match tag {
            KEY_TAG_NULL => Value::Null,
            KEY_TAG_INTEGER => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                Value::Integer(i64::from_le_bytes(raw.try_into().expect("8 bytes")))
            }
            KEY_TAG_TEXT => {
                let len = u32::from_le_bytes(
                    read_exact(bytes, &mut offset, 4)?
                        .try_into()
                        .expect("4 bytes"),
                ) as usize;
                let raw = read_exact(bytes, &mut offset, len)?;
                Value::Text(
                    String::from_utf8(raw.to_vec())
                        .map_err(|_| corrupt_row("key text is not valid UTF-8"))?,
                )
            }
            KEY_TAG_BOOLEAN => match read_exact(bytes, &mut offset, 1)?[0] {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                _ => return Err(corrupt_row("key boolean is not 0 or 1")),
            },
            KEY_TAG_DATE => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                Value::Date(i64::from_le_bytes(raw.try_into().expect("8 bytes")))
            }
            KEY_TAG_TIMESTAMP => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                Value::Timestamp(i64::from_le_bytes(raw.try_into().expect("8 bytes")))
            }
            _ => return Err(corrupt_row("unknown key value tag")),
        };
        values.push(value);
    }
    Ok((Key(values), offset))
}

/// Encode a freshly inserted row as a v2 tuple: `xmin = txn_id` (its creator),
/// `xmax = INVALID_XID` (live), `t_ctid = INVALID_TID` (no successor yet), and
/// `infomask = 0` (no settled-status or HOT hints). `txn_id` flows from the
/// inserting statement's `StatementContext.txn_id`.
pub fn encode_row(schema: &TableSchema, row: &Row, txn_id: TxnId) -> Result<Vec<u8>> {
    encode_row_with_infomask(schema, row, txn_id, 0)
}

/// Like [`encode_row`] but stamps an explicit `infomask` into the freshly inserted
/// tuple's header (the rest of the header is the fresh-insert default: `xmin =
/// txn_id`, `xmax = INVALID_XID`, `t_ctid = INVALID_TID`). The HOT-update fast path
/// (`mvcc.md` §10 Milestone H2) uses this to write a heap-only successor with
/// [`HEAP_ONLY`] already set in its header, so the bit is carried into the logged
/// `HeapInsert` image and redone on recovery (the row bytes are the source of truth
/// for `infomask`).
pub(crate) fn encode_row_with_infomask(
    schema: &TableSchema,
    row: &Row,
    txn_id: TxnId,
    infomask: u16,
) -> Result<Vec<u8>> {
    if row.values.len() != schema.columns.len() {
        return Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!(
                "row has {} values but table {} has {} columns",
                row.values.len(),
                schema.name,
                schema.columns.len()
            ),
        ));
    }

    let bitmap_len = null_bitmap_len(schema.columns.len());
    let mut bytes = vec![0; 1 + V2_MVCC_HEADER_LEN + bitmap_len];
    bytes[0] = ROW_FORMAT_VERSION;
    write_v2_header(&mut bytes[1..1 + V2_MVCC_HEADER_LEN], txn_id);
    // Stamp the requested infomask over the fresh-insert default (0). HOT uses this
    // to set HEAP_ONLY on the new heap-only tuple; the default path passes 0.
    bytes[1..3].copy_from_slice(&infomask.to_le_bytes());

    let bitmap_start = 1 + V2_MVCC_HEADER_LEN;
    let bitmap_end = bitmap_start + bitmap_len;
    for (index, (column, value)) in schema.columns.iter().zip(&row.values).enumerate() {
        match value {
            Value::Null => {
                if !column.nullable {
                    return Err(DbError::storage(
                        SqlState::NotNullViolation,
                        format!("column {} cannot be NULL", column.name),
                    ));
                }
                set_null(&mut bytes[bitmap_start..bitmap_end], index);
            }
            Value::Integer(value) if column.data_type == DataType::Integer => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Text(value) if column.data_type == DataType::Text => {
                let len = u32::try_from(value.len())
                    .map_err(|_| DbError::storage(SqlState::InternalError, "text is too large"))?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
            Value::Boolean(value) if column.data_type == DataType::Boolean => {
                bytes.push(u8::from(*value));
            }
            Value::Date(value) if column.data_type == DataType::Date => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Timestamp(value) if column.data_type == DataType::Timestamp => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            _ => {
                return Err(DbError::storage(
                    SqlState::DatatypeMismatch,
                    format!("value type does not match column {}", column.name),
                ));
            }
        }
    }

    Ok(bytes)
}

pub fn decode_row(schema: &TableSchema, bytes: &[u8]) -> Result<DecodedRow> {
    let bitmap_len = null_bitmap_len(schema.columns.len());
    if bytes.is_empty() {
        return Err(corrupt_row("row is shorter than its header"));
    }

    // Branch on the version byte: v2 carries the MVCC header before the null
    // bitmap; v1 has only the bitmap and synthesizes a frozen, never-deleted
    // header so pre-MVCC tuples are visible to every snapshot.
    let (xmin, xmax, t_ctid, infomask, header_len) = match bytes[0] {
        ROW_FORMAT_VERSION => {
            let header_len = 1 + V2_MVCC_HEADER_LEN + bitmap_len;
            if bytes.len() < header_len {
                return Err(corrupt_row("row is shorter than its header"));
            }
            let (xmin, xmax, t_ctid, infomask) = read_v2_header(&bytes[1..1 + V2_MVCC_HEADER_LEN])?;
            (xmin, xmax, t_ctid, infomask, header_len)
        }
        ROW_FORMAT_VERSION_V1 => {
            let header_len = 1 + bitmap_len;
            if bytes.len() < header_len {
                return Err(corrupt_row("row is shorter than its header"));
            }
            (
                FROZEN_XID,
                INVALID_XID,
                INVALID_TID,
                XMIN_COMMITTED,
                header_len,
            )
        }
        other => {
            return Err(corrupt_row(format!(
                "unsupported row format version {other}"
            )));
        }
    };

    let null_bitmap = &bytes[header_len - bitmap_len..header_len];
    let mut offset = header_len;
    let mut values = Vec::with_capacity(schema.columns.len());

    for (index, column) in schema.columns.iter().enumerate() {
        if is_null(null_bitmap, index) {
            values.push(Value::Null);
            continue;
        }

        let value = match column.data_type {
            DataType::Integer => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Integer(i64::from_le_bytes(array))
            }
            DataType::Text => {
                let raw_len = read_exact(bytes, &mut offset, 4)?;
                let mut array = [0; 4];
                array.copy_from_slice(raw_len);
                let len = u32::from_le_bytes(array) as usize;
                let raw = read_exact(bytes, &mut offset, len)?;
                let text = String::from_utf8(raw.to_vec())
                    .map_err(|_| corrupt_row("text value is not valid UTF-8"))?;
                Value::Text(text)
            }
            DataType::Boolean => {
                let raw = read_exact(bytes, &mut offset, 1)?[0];
                match raw {
                    0 => Value::Boolean(false),
                    1 => Value::Boolean(true),
                    _ => return Err(corrupt_row("boolean value is not 0 or 1")),
                }
            }
            DataType::Date => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Date(i64::from_le_bytes(array))
            }
            DataType::Timestamp => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Timestamp(i64::from_le_bytes(array))
            }
        };
        values.push(value);
    }

    if offset != bytes.len() {
        return Err(corrupt_row("row has trailing bytes"));
    }

    Ok(DecodedRow {
        row: Row { values },
        xmin,
        xmax,
        t_ctid,
        infomask,
    })
}

/// Decode just the MVCC header of a tuple buffer (version byte included),
/// returning `(xmin, xmax, t_ctid, infomask)` without needing a schema or
/// decoding the column payloads. Mirrors the header branch of [`decode_row`]: a v1
/// tuple synthesizes a frozen, never-deleted header (always visible / unlocked).
///
/// The write-write conflict check (`stamp_xmax_logged`, `docs/specs/mvcc.md` §7.3)
/// uses this to read the target version's *current physical* `xmax`/`infomask`
/// under the page's write latch, immediately before stamping — it only needs the
/// header fields, not the row values, and has no schema in hand.
pub(crate) fn decode_mvcc_header(tuple: &[u8]) -> Result<(TxnId, TxnId, (PageNum, u16), u16)> {
    match tuple.first() {
        Some(&ROW_FORMAT_VERSION) => {
            let header = tuple
                .get(1..1 + V2_MVCC_HEADER_LEN)
                .ok_or_else(|| corrupt_row("tuple is shorter than its v2 header"))?;
            read_v2_header(header)
        }
        Some(&ROW_FORMAT_VERSION_V1) => Ok((FROZEN_XID, INVALID_XID, INVALID_TID, XMIN_COMMITTED)),
        Some(other) => Err(corrupt_row(format!(
            "unsupported row format version {other}"
        ))),
        None => Err(corrupt_row("row is shorter than its header")),
    }
}

/// Write the v2 MVCC header into `header` (exactly `V2_MVCC_HEADER_LEN` bytes):
/// `[infomask:2][xmin:8][xmax:8][t_ctid:6]` for a freshly inserted tuple.
fn write_v2_header(header: &mut [u8], txn_id: TxnId) {
    debug_assert_eq!(header.len(), V2_MVCC_HEADER_LEN);
    header[0..2].copy_from_slice(&0u16.to_le_bytes());
    header[2..10].copy_from_slice(&txn_id.to_le_bytes());
    header[10..18].copy_from_slice(&INVALID_XID.to_le_bytes());
    let (page, slot) = INVALID_TID;
    header[18..22].copy_from_slice(&page.to_le_bytes());
    header[22..24].copy_from_slice(&slot.to_le_bytes());
}

/// Read the v2 MVCC header (`[infomask:2][xmin:8][xmax:8][t_ctid:6]`) from
/// exactly `V2_MVCC_HEADER_LEN` bytes, returning `(xmin, xmax, t_ctid, infomask)`.
fn read_v2_header(header: &[u8]) -> Result<(TxnId, TxnId, (PageNum, u16), u16)> {
    if header.len() != V2_MVCC_HEADER_LEN {
        return Err(corrupt_row("row v2 header has the wrong length"));
    }
    let infomask = u16::from_le_bytes(header[0..2].try_into().expect("2 bytes"));
    let xmin = u64::from_le_bytes(header[2..10].try_into().expect("8 bytes"));
    let xmax = u64::from_le_bytes(header[10..18].try_into().expect("8 bytes"));
    let page = u32::from_le_bytes(header[18..22].try_into().expect("4 bytes"));
    let slot = u16::from_le_bytes(header[22..24].try_into().expect("2 bytes"));
    Ok((xmin, xmax, (page, slot), infomask))
}

/// Mutate the in-place MVCC header fields of an existing v2 tuple, overwriting
/// `xmax`, `t_ctid`, and `infomask` in `tuple` (the full tuple byte buffer,
/// version byte included). `xmin` is the immutable creator and is left untouched.
///
/// These three are fixed-width header fields, so the tuple length is unchanged —
/// the heap page can rewrite them without relocating the tuple or compacting the
/// page. This is the single codec chokepoint for header-field offsets, so
/// `page.rs` mutates a tuple header through here rather than duplicating layout.
///
/// Returns `InternalError` if the buffer is not a v2 tuple or is shorter than the
/// v2 header, so misuse surfaces as a structured `DbError` instead of a panic.
///
/// Reachable via `page::set_tuple_header` ← `apply_physical_redo`'s
/// `HeapUpdateHeader` arm; the engine's `UPDATE`/`DELETE` emission paths arrive
/// in Milestone B commits 8–9.
pub(crate) fn set_mvcc_header_fields(
    tuple: &mut [u8],
    xmax: TxnId,
    t_ctid: (PageNum, u16),
    infomask: u16,
) -> Result<()> {
    if tuple.is_empty() || tuple[0] != ROW_FORMAT_VERSION {
        return Err(corrupt_row(
            "cannot mutate header of a non-v2 (or empty) tuple",
        ));
    }
    let header = tuple
        .get_mut(1..1 + V2_MVCC_HEADER_LEN)
        .ok_or_else(|| corrupt_row("tuple is shorter than its v2 header"))?;
    // Offsets are relative to the header slice: infomask[0..2], xmin[2..10]
    // (untouched), xmax[10..18], t_ctid.page[18..22], t_ctid.slot[22..24].
    header[0..2].copy_from_slice(&infomask.to_le_bytes());
    header[10..18].copy_from_slice(&xmax.to_le_bytes());
    let (page, slot) = t_ctid;
    header[18..22].copy_from_slice(&page.to_le_bytes());
    header[22..24].copy_from_slice(&slot.to_le_bytes());
    Ok(())
}

fn null_bitmap_len(columns: usize) -> usize {
    columns.div_ceil(8)
}

fn set_null(bitmap: &mut [u8], index: usize) {
    bitmap[index / 8] |= 1 << (index % 8);
}

fn is_null(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt_row("row offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt_row("row ended unexpectedly"))?;
    *offset = end;
    Ok(raw)
}

fn corrupt_row(message: impl Into<String>) -> common::DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use common::{
        ColumnDef, DataType, FROZEN_XID, INVALID_XID, Key, Row, TableSchema, Value, XMAX_COMMITTED,
        XMIN_COMMITTED,
    };

    use super::{
        INVALID_TID, ROW_FORMAT_VERSION, ROW_FORMAT_VERSION_V1, V2_MVCC_HEADER_LEN, decode_key,
        decode_row, encode_key, encode_row, null_bitmap_len, set_mvcc_header_fields,
    };

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
                },
                ColumnDef {
                    id: 1,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                },
            ],
            primary_key: vec![0],
        }
    }

    /// Build a legacy v1 tuple buffer (`[version=1][null_bitmap][columns]`) by
    /// hand so the v1 backward-compatibility path has a real input to decode.
    fn encode_row_v1(schema: &TableSchema, row: &Row) -> Vec<u8> {
        let bitmap_len = null_bitmap_len(schema.columns.len());
        let mut bytes = vec![0u8; 1 + bitmap_len];
        bytes[0] = ROW_FORMAT_VERSION_V1;
        for (index, value) in row.values.iter().enumerate() {
            match value {
                Value::Null => bytes[1 + index / 8] |= 1 << (index % 8),
                Value::Integer(value) => bytes.extend_from_slice(&value.to_le_bytes()),
                Value::Text(value) => {
                    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                }
                Value::Boolean(value) => bytes.push(u8::from(*value)),
                Value::Date(value) => bytes.extend_from_slice(&value.to_le_bytes()),
                Value::Timestamp(value) => bytes.extend_from_slice(&value.to_le_bytes()),
            }
        }
        bytes
    }

    #[test]
    fn row_format_version_is_two() {
        assert_eq!(ROW_FORMAT_VERSION, 2);
    }

    #[test]
    fn encode_prefixes_row_format_version() {
        let row = Row {
            values: vec![Value::Integer(7), Value::Null],
        };
        let bytes = encode_row(&schema(), &row, 11).unwrap();
        assert_eq!(bytes[0], ROW_FORMAT_VERSION);
    }

    #[test]
    fn v2_round_trip_preserves_header_and_values_including_nulls() {
        let row = Row {
            values: vec![Value::Integer(42), Value::Null],
        };
        let bytes = encode_row(&schema(), &row, 7).unwrap();
        let decoded = decode_row(&schema(), &bytes).unwrap();

        assert_eq!(decoded.row, row);
        assert_eq!(decoded.xmin, 7);
        assert_eq!(decoded.xmax, INVALID_XID);
        assert_eq!(decoded.t_ctid, INVALID_TID);
        assert_eq!(decoded.infomask, 0);
    }

    #[test]
    fn v2_header_occupies_the_documented_byte_width() {
        let row = Row {
            values: vec![Value::Integer(1), Value::Null],
        };
        let bytes = encode_row(&schema(), &row, 1).unwrap();
        // version byte + MVCC header + null bitmap precede the first column.
        assert!(bytes.len() >= 1 + V2_MVCC_HEADER_LEN + null_bitmap_len(schema().columns.len()));
    }

    #[test]
    fn v1_buffer_decodes_as_frozen_visible_row() {
        let row = Row {
            values: vec![Value::Integer(9), Value::Text("legacy".to_string())],
        };
        let bytes = encode_row_v1(&schema(), &row);
        let decoded = decode_row(&schema(), &bytes).unwrap();

        assert_eq!(decoded.row, row);
        assert_eq!(decoded.xmin, FROZEN_XID);
        assert_eq!(decoded.xmax, INVALID_XID);
        assert_eq!(decoded.t_ctid, INVALID_TID);
        assert_eq!(decoded.infomask, XMIN_COMMITTED);
    }

    #[test]
    fn v1_buffer_with_null_decodes_correctly() {
        let row = Row {
            values: vec![Value::Integer(3), Value::Null],
        };
        let bytes = encode_row_v1(&schema(), &row);
        let decoded = decode_row(&schema(), &bytes).unwrap();

        assert_eq!(decoded.row, row);
        assert_eq!(decoded.xmin, FROZEN_XID);
    }

    #[test]
    fn set_mvcc_header_fields_overwrites_only_xmax_t_ctid_infomask() {
        let row = Row {
            values: vec![Value::Integer(42), Value::Text("keep".to_string())],
        };
        let mut bytes = encode_row(&schema(), &row, 7).unwrap();

        set_mvcc_header_fields(&mut bytes, 99, (4, 5), XMAX_COMMITTED).unwrap();
        let decoded = decode_row(&schema(), &bytes).unwrap();

        // The three mutated header fields took the new values.
        assert_eq!(decoded.xmax, 99);
        assert_eq!(decoded.t_ctid, (4, 5));
        assert_eq!(decoded.infomask, XMAX_COMMITTED);
        // xmin (creator) and the column payload/null bitmap are undisturbed.
        assert_eq!(decoded.xmin, 7);
        assert_eq!(decoded.row, row);
    }

    #[test]
    fn set_mvcc_header_fields_rejects_non_v2_tuple() {
        let row = Row {
            values: vec![Value::Integer(1), Value::Null],
        };
        let mut bytes = encode_row_v1(&schema(), &row);
        assert!(set_mvcc_header_fields(&mut bytes, 5, INVALID_TID, 0).is_err());
        assert!(set_mvcc_header_fields(&mut [], 5, INVALID_TID, 0).is_err());
    }

    #[test]
    fn key_codec_round_trips_mixed_value_types() {
        let key = Key(vec![
            Value::Integer(-9),
            Value::Text("名前".to_string()),
            Value::Boolean(true),
        ]);
        let bytes = encode_key(&key).unwrap();
        assert_eq!(decode_key(&bytes).unwrap(), key);
    }

    #[test]
    fn decode_key_rejects_trailing_bytes() {
        let mut bytes = encode_key(&Key(vec![Value::Integer(1)])).unwrap();
        bytes.push(0);
        assert!(decode_key(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_unknown_row_format_version() {
        let row = Row {
            values: vec![Value::Integer(7), Value::Null],
        };
        let mut bytes = encode_row(&schema(), &row, 1).unwrap();
        bytes[0] = ROW_FORMAT_VERSION + 1;

        let err = decode_row(&schema(), &bytes).unwrap_err();
        assert!(err.message.contains("row format version"));
    }
}
