use common::{
    ArrayDimension, CheckedSliceReader, ColumnDef, DataType, DbError, Decimal, FROZEN_XID,
    INVALID_XID, Key, PageNum, Result, Row, SqlArray, SqlState, TableSchema, TxnId, Value,
    XMIN_COMMITTED,
};

/// On-page row encoding version emitted by the legacy `encode_row` helper. The
/// storage engine's TOAST-aware DML path emits prepared row format v3. The MVCC
/// layout (v2) is
/// `[version=2][infomask:2][xmin:8][xmax:8][t_ctid:6][null_bitmap][columns]`.
/// Row format v3 keeps the same MVCC header and null bitmap, but uses tagged
/// varlena length words for `TEXT`/`BYTEA`. The legacy pre-MVCC layout (v1) is
/// `[version=1][null_bitmap][columns]`. `decode_row` reads all three so old heaps
/// keep decoding while the TOAST write path is introduced in phases.
const ROW_FORMAT_VERSION: u8 = ROW_FORMAT_VERSION_V2;

/// Legacy pre-MVCC row layout: `[version=1][null_bitmap][columns]` with no
/// per-version transaction header. Still decoded for backward compatibility.
const ROW_FORMAT_VERSION_V1: u8 = 1;
const ROW_FORMAT_VERSION_V2: u8 = 2;
pub(crate) const ROW_FORMAT_VERSION_V3: u8 = 3;

/// Byte width of the v2 MVCC header that precedes the v1-style null bitmap:
/// `infomask(2) + xmin(8) + xmax(8) + t_ctid(6)`. The version byte and null
/// bitmap are accounted for separately.
pub(crate) const V2_MVCC_HEADER_LEN: usize = 2 + 8 + 8 + 6;

pub(crate) const VARLENA_TAG_SHIFT: u32 = 30;
pub(crate) const VARLENA_LEN_MASK: u32 = (1 << VARLENA_TAG_SHIFT) - 1;
pub(crate) const TAG_PLAIN: u8 = 0;
pub(crate) const TAG_COMPRESSED: u8 = 1;
pub(crate) const TAG_EXTERNAL: u8 = 2;
pub(crate) const TOAST_POINTER_LEN: usize = 17;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MvccHeader {
    pub xmin: TxnId,
    pub xmax: TxnId,
    pub t_ctid: (PageNum, u16),
    pub infomask: u16,
}

impl MvccHeader {
    pub(crate) fn fresh(txn_id: TxnId, infomask: u16) -> Self {
        Self {
            xmin: txn_id,
            xmax: INVALID_XID,
            t_ctid: INVALID_TID,
            infomask,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ToastPointer {
    pub value_id: u64,
    pub raw_len: u32,
    pub stored_len: u32,
    pub codec: u8,
}

impl ToastPointer {
    pub(crate) fn encode(&self) -> Result<[u8; TOAST_POINTER_LEN]> {
        validate_toast_pointer_value_id(self.value_id)?;
        validate_varlena_u32_len(self.raw_len, "toast pointer raw length")?;
        validate_varlena_u32_len(self.stored_len, "toast pointer stored length")?;
        validate_toast_pointer_codec(self.codec)?;

        let mut bytes = [0u8; TOAST_POINTER_LEN];
        bytes[0..8].copy_from_slice(&self.value_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.raw_len.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.stored_len.to_le_bytes());
        bytes[16] = self.codec;
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != TOAST_POINTER_LEN {
            return Err(corrupt_row(format!(
                "toast pointer has {} bytes, expected {TOAST_POINTER_LEN}",
                bytes.len()
            )));
        }
        let mut offset = 0;
        let pointer = Self {
            value_id: u64::from_le_bytes(read_array(bytes, &mut offset)?),
            raw_len: u32::from_le_bytes(read_array(bytes, &mut offset)?),
            stored_len: u32::from_le_bytes(read_array(bytes, &mut offset)?),
            codec: read_u8(bytes, &mut offset)?,
        };
        validate_toast_pointer_value_id(pointer.value_id)?;
        validate_decoded_varlena_u32_len(pointer.raw_len, "toast pointer raw length")?;
        validate_decoded_varlena_u32_len(pointer.stored_len, "toast pointer stored length")?;
        validate_toast_pointer_codec(pointer.codec)?;
        Ok(pointer)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VarlenaPhysical {
    Plain(Vec<u8>),
    Compressed {
        codec: u8,
        dict_id: Option<u32>,
        raw_len: u32,
        raw_crc32: u32,
        payload: Vec<u8>,
    },
    External(ToastPointer),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedColumnValue {
    Null,
    Value(Value),
    Varlena(VarlenaPhysical),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedPhysicalRow {
    pub header: MvccHeader,
    pub values: Vec<DecodedPhysicalValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodedPhysicalValue {
    Null,
    Value(Value),
    Compressed {
        column: usize,
        codec: u8,
        dict_id: Option<u32>,
        raw_len: u32,
        raw_crc32: u32,
        payload: Vec<u8>,
    },
    External {
        column: usize,
        pointer: ToastPointer,
    },
}

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
const KEY_TAG_BYTEA: u8 = 6;
const KEY_TAG_UUID: u8 = 7;
const KEY_TAG_DOUBLE: u8 = 8;
const KEY_TAG_NUMERIC: u8 = 9;
const KEY_TAG_REAL: u8 = 10;
const KEY_TAG_TIME: u8 = 11;
const KEY_TAG_TIMESTAMPTZ: u8 = 12;
const KEY_TAG_INTERVAL: u8 = 13;
const KEY_TAG_ARRAY: u8 = 14;
const ARRAY_PAYLOAD_VERSION: u8 = 1;
pub(crate) const MAX_ARRAY_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Serialize an `INTERVAL` as `[months: i32][days: i32][micros: i64]` (16 bytes LE).
fn put_interval(bytes: &mut Vec<u8>, value: &common::Interval) {
    bytes.extend_from_slice(&value.months.to_le_bytes());
    bytes.extend_from_slice(&value.days.to_le_bytes());
    bytes.extend_from_slice(&value.micros.to_le_bytes());
}

/// Decode an `INTERVAL` written by [`put_interval`].
fn read_interval(bytes: &[u8], offset: &mut usize) -> Result<common::Interval> {
    let months = i32::from_le_bytes(read_array(bytes, offset)?);
    let days = i32::from_le_bytes(read_array(bytes, offset)?);
    let micros = i64::from_le_bytes(read_array(bytes, offset)?);
    Ok(common::Interval::new(months, days, micros))
}

/// Serialize a `NUMERIC` value as its exact `i128` mantissa (16 bytes LE) plus
/// `u32` scale (4 bytes LE) — a fixed 20 bytes that preserves value and scale.
fn put_numeric(bytes: &mut Vec<u8>, value: &Decimal) {
    bytes.extend_from_slice(&value.mantissa().to_le_bytes());
    bytes.extend_from_slice(&value.scale().to_le_bytes());
}

/// Decode a `NUMERIC` value written by [`put_numeric`].
fn read_numeric(bytes: &[u8], offset: &mut usize) -> Result<Decimal> {
    let mantissa = i128::from_le_bytes(read_array(bytes, offset)?);
    let scale = u32::from_le_bytes(read_array(bytes, offset)?);
    Decimal::try_from_i128_with_scale(mantissa, scale)
        .map_err(|_| corrupt_row("invalid numeric value"))
}

pub(crate) fn encode_array_payload(array: &SqlArray) -> Result<Vec<u8>> {
    let encoded_len = encoded_array_payload_len(array)?;
    let cardinality = u32::try_from(array.cardinality()).map_err(|_| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "array has too many elements",
        )
    })?;
    let ndim = u8::try_from(array.dimensions().len()).map_err(|_| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "array has too many dimensions",
        )
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(encoded_len)
        .map_err(|_| array_size_error("array payload memory reservation failed"))?;
    bytes.push(ARRAY_PAYLOAD_VERSION);
    encode_array_element_type(&mut bytes, array.element_type())?;
    bytes.push(ndim);
    bytes.extend_from_slice(&cardinality.to_le_bytes());
    for dimension in array.dimensions() {
        bytes.extend_from_slice(&dimension.len().to_le_bytes());
        bytes.extend_from_slice(&dimension.lower_bound().to_le_bytes());
    }
    let bitmap_len = array.cardinality().div_ceil(8);
    let bitmap_start = bytes.len();
    let bitmap_end = bitmap_start
        .checked_add(bitmap_len)
        .ok_or_else(|| array_size_error("array bitmap range overflows"))?;
    bytes.resize(bitmap_end, 0);
    for (index, value) in array.elements().iter().enumerate() {
        if matches!(value, Value::Null) {
            let bitmap = bytes
                .get_mut(bitmap_start..bitmap_end)
                .ok_or_else(|| DbError::internal("array bitmap range is outside encoded bytes"))?;
            set_null(bitmap, index)?;
        } else {
            encode_array_element(&mut bytes, array.element_type(), value)?;
        }
    }
    if bytes.len() != encoded_len {
        return Err(DbError::internal(
            "encoded array length did not match its preflight size",
        ));
    }
    Ok(bytes)
}

pub(crate) fn encoded_array_payload_len(array: &SqlArray) -> Result<usize> {
    let type_len = 1 + usize::from(matches!(array.element_type(), DataType::Numeric { .. })) * 8;
    let mut len = 1_usize
        .checked_add(type_len)
        .and_then(|len| len.checked_add(1 + 4))
        .and_then(|len| len.checked_add(array.dimensions().len() * 8))
        .and_then(|len| len.checked_add(array.cardinality().div_ceil(8)))
        .ok_or_else(|| array_size_error("array payload length overflows"))?;
    for value in array.elements() {
        let element_len = match value {
            Value::Null => Some(0),
            Value::Integer(_)
            | Value::Date(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::TimestampTz(_)
            | Value::Float(_) => Some(8),
            Value::Text(value) => 4_usize.checked_add(value.len()),
            Value::Boolean(_) => Some(1),
            Value::Interval(_) | Value::Uuid(_) => Some(16),
            Value::Bytes(value) => 4_usize.checked_add(value.len()),
            Value::Numeric(_) => Some(20),
            Value::Real(_) => Some(4),
            Value::Array(_) => None,
        }
        .ok_or_else(|| array_size_error("array element length overflows"))?;
        len = len
            .checked_add(element_len)
            .ok_or_else(|| array_size_error("array payload length overflows"))?;
    }
    if len > MAX_ARRAY_PAYLOAD_BYTES {
        Err(array_size_error(
            "array payload exceeds the supported length",
        ))
    } else {
        Ok(len)
    }
}

fn array_size_error(message: &'static str) -> DbError {
    DbError::storage(SqlState::ProgramLimitExceeded, message)
}

pub(crate) fn decode_array_payload(bytes: &[u8]) -> Result<SqlArray> {
    validate_decoded_array_payload_len(bytes.len())?;
    let mut offset = 0;
    let version = read_u8(bytes, &mut offset)?;
    if version != ARRAY_PAYLOAD_VERSION {
        return Err(corrupt_row(format!(
            "unknown array payload version {version}"
        )));
    }
    let element_type = decode_array_element_type(bytes, &mut offset)?;
    let ndim = usize::from(read_u8(bytes, &mut offset)?);
    if ndim > common::MAX_ARRAY_DIMENSIONS {
        return Err(corrupt_row("array has too many dimensions"));
    }
    let cardinality = usize::try_from(read_u32(bytes, &mut offset)?)
        .map_err(|_| corrupt_row("array cardinality does not fit usize"))?;
    if cardinality > common::MAX_ARRAY_ELEMENTS {
        return Err(corrupt_row("array cardinality exceeds the supported limit"));
    }
    let mut dimensions = fallible_decode_vec(ndim, "array dimensions")?;
    for _ in 0..ndim {
        let len = read_u32(bytes, &mut offset)?;
        let lower_bound = i32::from_le_bytes(read_array(bytes, &mut offset)?);
        dimensions.push(ArrayDimension::new(len, lower_bound));
    }
    if cardinality == 0 && ndim != 0 {
        return Err(corrupt_row("empty array payload must have zero dimensions"));
    }
    if cardinality != 0 && ndim == 0 {
        return Err(corrupt_row("non-empty array payload must have dimensions"));
    }
    let described = dimensions.iter().try_fold(1_usize, |product, dimension| {
        product
            .checked_mul(
                usize::try_from(dimension.len())
                    .map_err(|_| corrupt_row("array dimension does not fit usize"))?,
            )
            .ok_or_else(|| corrupt_row("array dimension product overflows"))
    })?;
    if cardinality != 0 && described != cardinality {
        return Err(corrupt_row("array dimensions do not match cardinality"));
    }
    let bitmap_len = cardinality.div_ceil(8);
    let bitmap = read_exact(bytes, &mut offset, bitmap_len)?;
    if !cardinality.is_multiple_of(8)
        && bitmap
            .last()
            .is_some_and(|last| *last & !((1_u8 << (cardinality % 8)) - 1) != 0)
    {
        return Err(corrupt_row("array null bitmap has nonzero padding bits"));
    }
    let mut elements = fallible_decode_vec(cardinality, "array elements")?;
    for index in 0..cardinality {
        if is_null(bitmap, index)? {
            elements.push(Value::Null);
        } else {
            elements.push(decode_array_element(bytes, &mut offset, &element_type)?);
        }
    }
    if offset != bytes.len() {
        return Err(corrupt_row("array payload has trailing bytes"));
    }
    SqlArray::new(element_type, dimensions, elements)
        .map_err(|error| corrupt_row(format!("invalid array payload: {}", error.message)))
}

pub(crate) fn validate_decoded_array_payload_len(len: usize) -> Result<()> {
    if len > MAX_ARRAY_PAYLOAD_BYTES {
        Err(corrupt_row("array payload exceeds the supported length"))
    } else {
        Ok(())
    }
}

fn encode_array_element_type(bytes: &mut Vec<u8>, data_type: &DataType) -> Result<()> {
    validate_array_numeric_type(data_type, false)?;
    let tag = match data_type {
        DataType::Integer => 0,
        DataType::Text => 1,
        DataType::Boolean => 2,
        DataType::Date => 3,
        DataType::Timestamp => 4,
        DataType::Time => 5,
        DataType::TimestampTz => 6,
        DataType::Interval => 7,
        DataType::Bytea => 8,
        DataType::Uuid => 9,
        DataType::Double => 10,
        DataType::Real => 11,
        DataType::Numeric { .. } => 12,
        DataType::Array(_) => {
            return Err(DbError::storage(
                SqlState::DatatypeMismatch,
                "nested array element type is invalid",
            ));
        }
    };
    bytes.push(tag);
    if let DataType::Numeric { precision, scale } = data_type {
        bytes.extend_from_slice(&precision.unwrap_or(u32::MAX).to_le_bytes());
        bytes.extend_from_slice(&scale.to_le_bytes());
    }
    Ok(())
}

fn decode_array_element_type(bytes: &[u8], offset: &mut usize) -> Result<DataType> {
    let data_type = match read_u8(bytes, offset)? {
        0 => DataType::Integer,
        1 => DataType::Text,
        2 => DataType::Boolean,
        3 => DataType::Date,
        4 => DataType::Timestamp,
        5 => DataType::Time,
        6 => DataType::TimestampTz,
        7 => DataType::Interval,
        8 => DataType::Bytea,
        9 => DataType::Uuid,
        10 => DataType::Double,
        11 => DataType::Real,
        12 => {
            let precision = read_u32(bytes, offset)?;
            let scale = read_u32(bytes, offset)?;
            DataType::Numeric {
                precision: (precision != u32::MAX).then_some(precision),
                scale,
            }
        }
        _ => return Err(corrupt_row("unknown array element type tag")),
    };
    validate_array_numeric_type(&data_type, true)?;
    Ok(data_type)
}

fn validate_array_numeric_type(data_type: &DataType, decoded: bool) -> Result<()> {
    let DataType::Numeric { precision, scale } = data_type else {
        return Ok(());
    };
    let valid = match precision {
        Some(precision) => (1..=28).contains(precision) && scale <= precision,
        None => *scale == 0,
    };
    if valid {
        Ok(())
    } else if decoded {
        Err(corrupt_row("invalid numeric array element type modifier"))
    } else {
        Err(DbError::storage(
            SqlState::DatatypeMismatch,
            "invalid numeric array element type modifier",
        ))
    }
}

fn encode_array_element(bytes: &mut Vec<u8>, data_type: &DataType, value: &Value) -> Result<()> {
    match (data_type, value) {
        (DataType::Integer, Value::Integer(value))
        | (DataType::Date, Value::Date(value))
        | (DataType::Timestamp, Value::Timestamp(value))
        | (DataType::Time, Value::Time(value))
        | (DataType::TimestampTz, Value::TimestampTz(value)) => {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        (DataType::Text, Value::Text(value)) => put_array_bytes(bytes, value.as_bytes())?,
        (DataType::Boolean, Value::Boolean(value)) => bytes.push(u8::from(*value)),
        (DataType::Interval, Value::Interval(value)) => put_interval(bytes, value),
        (DataType::Bytea, Value::Bytes(value)) => put_array_bytes(bytes, value)?,
        (DataType::Uuid, Value::Uuid(value)) => bytes.extend_from_slice(value),
        (DataType::Double, Value::Float(value)) => bytes.extend_from_slice(&value.0.to_le_bytes()),
        (DataType::Real, Value::Real(value)) => bytes.extend_from_slice(&value.0.to_le_bytes()),
        (DataType::Numeric { .. }, Value::Numeric(value)) => put_numeric(bytes, value),
        _ => {
            return Err(DbError::storage(
                SqlState::DatatypeMismatch,
                "array element does not match its element type",
            ));
        }
    }
    Ok(())
}

fn decode_array_element(bytes: &[u8], offset: &mut usize, data_type: &DataType) -> Result<Value> {
    Ok(match data_type {
        DataType::Integer => Value::Integer(read_i64(bytes, offset)?),
        DataType::Text => Value::Text(
            String::from_utf8(read_array_bytes(bytes, offset)?.to_vec())
                .map_err(|_| corrupt_row("array text element is not valid UTF-8"))?,
        ),
        DataType::Boolean => match read_u8(bytes, offset)? {
            0 => Value::Boolean(false),
            1 => Value::Boolean(true),
            _ => return Err(corrupt_row("array boolean element is not 0 or 1")),
        },
        DataType::Date => Value::Date(read_i64(bytes, offset)?),
        DataType::Timestamp => Value::Timestamp(read_i64(bytes, offset)?),
        DataType::Time => Value::Time(read_i64(bytes, offset)?),
        DataType::TimestampTz => Value::TimestampTz(read_i64(bytes, offset)?),
        DataType::Interval => Value::Interval(read_interval(bytes, offset)?),
        DataType::Bytea => Value::Bytes(read_array_bytes(bytes, offset)?.to_vec()),
        DataType::Uuid => Value::Uuid(read_array(bytes, offset)?),
        DataType::Double => Value::Float(f64::from_le_bytes(read_array(bytes, offset)?).into()),
        DataType::Real => Value::Real(f32::from_le_bytes(read_array(bytes, offset)?).into()),
        DataType::Numeric { .. } => Value::Numeric(read_numeric(bytes, offset)?),
        DataType::Array(_) => return Err(corrupt_row("nested array element type is invalid")),
    })
}

fn put_array_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        DbError::storage(SqlState::ProgramLimitExceeded, "array element is too large")
    })?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_array_bytes<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let len = decoded_usize(read_u32(bytes, offset)?, "array element length")?;
    read_exact(bytes, offset, len)
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_array(bytes, offset)?))
}

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
                    DbError::storage(SqlState::ProgramLimitExceeded, "key text is too large")
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
            Value::Time(value) => {
                bytes.push(KEY_TAG_TIME);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::TimestampTz(value) => {
                bytes.push(KEY_TAG_TIMESTAMPTZ);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Interval(value) => {
                bytes.push(KEY_TAG_INTERVAL);
                put_interval(&mut bytes, value);
            }
            Value::Bytes(value) => {
                bytes.push(KEY_TAG_BYTEA);
                let len = u32::try_from(value.len()).map_err(|_| {
                    DbError::storage(SqlState::ProgramLimitExceeded, "key bytea is too large")
                })?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(value);
            }
            Value::Uuid(value) => {
                bytes.push(KEY_TAG_UUID);
                bytes.extend_from_slice(value);
            }
            Value::Float(value) => {
                bytes.push(KEY_TAG_DOUBLE);
                bytes.extend_from_slice(&value.0.to_le_bytes());
            }
            Value::Numeric(value) => {
                bytes.push(KEY_TAG_NUMERIC);
                put_numeric(&mut bytes, value);
            }
            Value::Real(value) => {
                bytes.push(KEY_TAG_REAL);
                bytes.extend_from_slice(&value.0.to_le_bytes());
            }
            Value::Array(array) => {
                let payload_len = encoded_array_payload_len(array)?;
                if payload_len > buffer::PAGE_SIZE {
                    return Err(DbError::storage(
                        SqlState::ProgramLimitExceeded,
                        "array key is too large for a B-tree page",
                    ));
                }
                bytes.push(KEY_TAG_ARRAY);
                let payload = encode_array_payload(array)?;
                if payload.len() != payload_len {
                    return Err(DbError::internal(
                        "encoded array key length did not match its preflight size",
                    ));
                }
                let len = u32::try_from(payload_len).map_err(|_| {
                    DbError::storage(SqlState::ProgramLimitExceeded, "array key is too large")
                })?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(&payload);
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
    let count = u16::from_le_bytes(read_array(bytes, &mut offset)?);
    let count = usize::from(count);
    let mut values = fallible_decode_vec(count, "key values")?;
    for _ in 0..count {
        let tag = read_u8(bytes, &mut offset)?;
        let value = match tag {
            KEY_TAG_NULL => Value::Null,
            KEY_TAG_INTEGER => Value::Integer(i64::from_le_bytes(read_array(bytes, &mut offset)?)),
            KEY_TAG_TEXT => {
                let len = decoded_usize(
                    u32::from_le_bytes(read_array(bytes, &mut offset)?),
                    "key text length",
                )?;
                let raw = read_exact(bytes, &mut offset, len)?;
                Value::Text(
                    String::from_utf8(raw.to_vec())
                        .map_err(|_| corrupt_row("key text is not valid UTF-8"))?,
                )
            }
            KEY_TAG_BOOLEAN => match read_u8(bytes, &mut offset)? {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                _ => return Err(corrupt_row("key boolean is not 0 or 1")),
            },
            KEY_TAG_DATE => Value::Date(i64::from_le_bytes(read_array(bytes, &mut offset)?)),
            KEY_TAG_TIMESTAMP => {
                Value::Timestamp(i64::from_le_bytes(read_array(bytes, &mut offset)?))
            }
            KEY_TAG_TIME => Value::Time(i64::from_le_bytes(read_array(bytes, &mut offset)?)),
            KEY_TAG_TIMESTAMPTZ => {
                Value::TimestampTz(i64::from_le_bytes(read_array(bytes, &mut offset)?))
            }
            KEY_TAG_INTERVAL => Value::Interval(read_interval(bytes, &mut offset)?),
            KEY_TAG_BYTEA => {
                let len = decoded_usize(
                    u32::from_le_bytes(read_array(bytes, &mut offset)?),
                    "key bytea length",
                )?;
                Value::Bytes(read_exact(bytes, &mut offset, len)?.to_vec())
            }
            KEY_TAG_UUID => Value::Uuid(read_array(bytes, &mut offset)?),
            KEY_TAG_DOUBLE => {
                Value::Float(f64::from_le_bytes(read_array(bytes, &mut offset)?).into())
            }
            KEY_TAG_NUMERIC => Value::Numeric(read_numeric(bytes, &mut offset)?),
            KEY_TAG_REAL => Value::Real(f32::from_le_bytes(read_array(bytes, &mut offset)?).into()),
            KEY_TAG_ARRAY => {
                let len = decoded_usize(read_u32(bytes, &mut offset)?, "key array length")?;
                Value::Array(decode_array_payload(read_exact(bytes, &mut offset, len)?)?)
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
    write_v2_header(&mut bytes[1..1 + V2_MVCC_HEADER_LEN], txn_id)?;
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
                let bitmap = bytes.get_mut(bitmap_start..bitmap_end).ok_or_else(|| {
                    DbError::internal("row bitmap range is outside encoded bytes")
                })?;
                set_null(bitmap, index)?;
            }
            Value::Integer(value) if column.data_type == DataType::Integer => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Float(value) if column.data_type == DataType::Double => {
                bytes.extend_from_slice(&value.0.to_le_bytes());
            }
            Value::Real(value) if column.data_type == DataType::Real => {
                bytes.extend_from_slice(&value.0.to_le_bytes());
            }
            Value::Numeric(value) if matches!(column.data_type, DataType::Numeric { .. }) => {
                put_numeric(&mut bytes, value);
            }
            Value::Text(value) if column.data_type == DataType::Text => {
                let len = u32::try_from(value.len()).map_err(|_| {
                    DbError::storage(SqlState::ProgramLimitExceeded, "text is too large")
                })?;
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
            Value::Time(value) if column.data_type == DataType::Time => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::TimestampTz(value) if column.data_type == DataType::TimestampTz => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Interval(value) if column.data_type == DataType::Interval => {
                put_interval(&mut bytes, value);
            }
            Value::Bytes(value) if column.data_type == DataType::Bytea => {
                let len = u32::try_from(value.len()).map_err(|_| {
                    DbError::storage(SqlState::ProgramLimitExceeded, "bytea is too large")
                })?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(value);
            }
            Value::Uuid(value) if column.data_type == DataType::Uuid => {
                bytes.extend_from_slice(value);
            }
            Value::Array(array) if matches!(&column.data_type, DataType::Array(element) if element.element_type() == array.element_type()) =>
            {
                let payload = encode_array_payload(array)?;
                let len = u32::try_from(payload.len()).map_err(|_| {
                    DbError::storage(SqlState::ProgramLimitExceeded, "array is too large")
                })?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(&payload);
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

#[allow(
    dead_code,
    reason = "called by the TOAST write path added after the v3 codec phase"
)]
pub(crate) fn encode_row_v3_prepared(
    schema: &TableSchema,
    header: &MvccHeader,
    physical_values: &[PreparedColumnValue],
) -> Result<Vec<u8>> {
    if physical_values.len() != schema.columns.len() {
        return Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!(
                "row has {} values but table {} has {} columns",
                physical_values.len(),
                schema.name,
                schema.columns.len()
            ),
        ));
    }

    let bitmap_len = null_bitmap_len(schema.columns.len());
    let mut bytes = vec![0; 1 + V2_MVCC_HEADER_LEN + bitmap_len];
    bytes[0] = ROW_FORMAT_VERSION_V3;
    write_mvcc_header(&mut bytes[1..1 + V2_MVCC_HEADER_LEN], header)?;

    let bitmap_start = 1 + V2_MVCC_HEADER_LEN;
    let bitmap_end = bitmap_start + bitmap_len;
    for (index, (column, value)) in schema.columns.iter().zip(physical_values).enumerate() {
        match value {
            PreparedColumnValue::Null => {
                if !column.nullable {
                    return Err(DbError::storage(
                        SqlState::NotNullViolation,
                        format!("column {} cannot be NULL", column.name),
                    ));
                }
                let bitmap = bytes.get_mut(bitmap_start..bitmap_end).ok_or_else(|| {
                    DbError::internal("row bitmap range is outside encoded bytes")
                })?;
                set_null(bitmap, index)?;
            }
            PreparedColumnValue::Value(value) => {
                encode_column_value_v3(&mut bytes, column, value)?;
            }
            PreparedColumnValue::Varlena(physical) => {
                encode_varlena_physical(
                    &mut bytes,
                    column.data_type.clone(),
                    schema.toast_table_id.is_some(),
                    physical,
                )?;
            }
        }
    }

    Ok(bytes)
}

pub fn decode_row(schema: &TableSchema, bytes: &[u8]) -> Result<DecodedRow> {
    let decoded = decode_physical_row(schema, bytes)?;
    let mut values = Vec::with_capacity(decoded.values.len());
    for value in decoded.values {
        match value {
            DecodedPhysicalValue::Null => values.push(Value::Null),
            DecodedPhysicalValue::Value(value) => values.push(value),
            DecodedPhysicalValue::Compressed { column, .. } => {
                return Err(corrupt_row(format!(
                    "column {column} is an inline compressed TOAST value"
                )));
            }
            DecodedPhysicalValue::External { column, .. } => {
                return Err(corrupt_row(format!(
                    "column {column} is an external TOAST value"
                )));
            }
        }
    }

    Ok(DecodedRow {
        row: Row { values },
        xmin: decoded.header.xmin,
        xmax: decoded.header.xmax,
        t_ctid: decoded.header.t_ctid,
        infomask: decoded.header.infomask,
    })
}

pub(crate) fn decode_physical_row(
    schema: &TableSchema,
    bytes: &[u8],
) -> Result<DecodedPhysicalRow> {
    let bitmap_len = null_bitmap_len(schema.columns.len());
    if bytes.is_empty() {
        return Err(corrupt_row("row is shorter than its header"));
    }

    // Branch on the version byte: v2/v3 carry the MVCC header before the null
    // bitmap; v1 has only the bitmap and synthesizes a frozen, never-deleted
    // header so pre-MVCC tuples are visible to every snapshot.
    let (version, header, header_len) = match bytes[0] {
        ROW_FORMAT_VERSION_V2 | ROW_FORMAT_VERSION_V3 => {
            let header_len = 1 + V2_MVCC_HEADER_LEN + bitmap_len;
            if bytes.len() < header_len {
                return Err(corrupt_row("row is shorter than its header"));
            }
            let header = read_mvcc_header(&bytes[1..1 + V2_MVCC_HEADER_LEN])?;
            (bytes[0], header, header_len)
        }
        ROW_FORMAT_VERSION_V1 => {
            let header_len = 1 + bitmap_len;
            if bytes.len() < header_len {
                return Err(corrupt_row("row is shorter than its header"));
            }
            (
                ROW_FORMAT_VERSION_V1,
                MvccHeader {
                    xmin: FROZEN_XID,
                    xmax: INVALID_XID,
                    t_ctid: INVALID_TID,
                    infomask: XMIN_COMMITTED,
                },
                header_len,
            )
        }
        other => {
            return Err(corrupt_row(format!(
                "unsupported row format version {other}"
            )));
        }
    };

    let bitmap_start = header_len
        .checked_sub(bitmap_len)
        .ok_or_else(|| corrupt_row("row bitmap begins before the row header"))?;
    let null_bitmap = bytes
        .get(bitmap_start..header_len)
        .ok_or_else(|| corrupt_row("row bitmap range is outside the tuple"))?;
    let mut offset = header_len;
    let mut values = Vec::with_capacity(schema.columns.len());

    for (index, column) in schema.columns.iter().enumerate() {
        if is_null(null_bitmap, index)? {
            values.push(DecodedPhysicalValue::Null);
            continue;
        }

        let value = match column.data_type {
            DataType::Integer => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Integer(i64::from_le_bytes(array))
            }
            DataType::Double => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Float(f64::from_le_bytes(array).into())
            }
            DataType::Numeric { .. } => Value::Numeric(read_numeric(bytes, &mut offset)?),
            DataType::Real => {
                let raw = read_exact(bytes, &mut offset, 4)?;
                let mut array = [0; 4];
                array.copy_from_slice(raw);
                Value::Real(f32::from_le_bytes(array).into())
            }
            DataType::Text => {
                values.push(decode_varlena_physical(
                    bytes,
                    &mut offset,
                    version,
                    index,
                    DataType::Text,
                    schema.toast_table_id.is_some(),
                )?);
                continue;
            }
            DataType::Boolean => {
                let raw = read_u8(bytes, &mut offset)?;
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
            DataType::Time => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Time(i64::from_le_bytes(array))
            }
            DataType::TimestampTz => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::TimestampTz(i64::from_le_bytes(array))
            }
            DataType::Interval => Value::Interval(read_interval(bytes, &mut offset)?),
            DataType::Bytea => {
                values.push(decode_varlena_physical(
                    bytes,
                    &mut offset,
                    version,
                    index,
                    DataType::Bytea,
                    schema.toast_table_id.is_some(),
                )?);
                continue;
            }
            DataType::Uuid => Value::Uuid(read_array(bytes, &mut offset)?),
            DataType::Array(_) => {
                values.push(decode_varlena_physical(
                    bytes,
                    &mut offset,
                    version,
                    index,
                    column.data_type.clone(),
                    schema.toast_table_id.is_some(),
                )?);
                continue;
            }
        };
        values.push(DecodedPhysicalValue::Value(value));
    }

    if offset != bytes.len() {
        return Err(corrupt_row("row has trailing bytes"));
    }

    Ok(DecodedPhysicalRow { header, values })
}

fn encode_column_value_v3(bytes: &mut Vec<u8>, column: &ColumnDef, value: &Value) -> Result<()> {
    match value {
        Value::Null => Err(DbError::storage(
            SqlState::InternalError,
            "NULL should be represented by PreparedColumnValue::Null",
        )),
        Value::Integer(value) if column.data_type == DataType::Integer => {
            bytes.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        Value::Float(value) if column.data_type == DataType::Double => {
            bytes.extend_from_slice(&value.0.to_le_bytes());
            Ok(())
        }
        Value::Real(value) if column.data_type == DataType::Real => {
            bytes.extend_from_slice(&value.0.to_le_bytes());
            Ok(())
        }
        Value::Numeric(value) if matches!(column.data_type, DataType::Numeric { .. }) => {
            put_numeric(bytes, value);
            Ok(())
        }
        Value::Text(value) if column.data_type == DataType::Text => {
            put_varlena(bytes, TAG_PLAIN, value.as_bytes())
        }
        Value::Boolean(value) if column.data_type == DataType::Boolean => {
            bytes.push(u8::from(*value));
            Ok(())
        }
        Value::Date(value) if column.data_type == DataType::Date => {
            bytes.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        Value::Timestamp(value) if column.data_type == DataType::Timestamp => {
            bytes.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        Value::Time(value) if column.data_type == DataType::Time => {
            bytes.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        Value::TimestampTz(value) if column.data_type == DataType::TimestampTz => {
            bytes.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        Value::Interval(value) if column.data_type == DataType::Interval => {
            put_interval(bytes, value);
            Ok(())
        }
        Value::Bytes(value) if column.data_type == DataType::Bytea => {
            put_varlena(bytes, TAG_PLAIN, value)
        }
        Value::Uuid(value) if column.data_type == DataType::Uuid => {
            bytes.extend_from_slice(value);
            Ok(())
        }
        Value::Array(array) if matches!(&column.data_type, DataType::Array(element) if element.element_type() == array.element_type()) => {
            put_varlena(bytes, TAG_PLAIN, &encode_array_payload(array)?)
        }
        _ => Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!("value type does not match column {}", column.name),
        )),
    }
}

#[allow(
    dead_code,
    reason = "called by encode_row_v3_prepared once the TOAST write path is wired in"
)]
fn encode_varlena_physical(
    bytes: &mut Vec<u8>,
    data_type: DataType,
    has_toast_relation: bool,
    physical: &VarlenaPhysical,
) -> Result<()> {
    if !matches!(
        data_type,
        DataType::Text | DataType::Bytea | DataType::Array(_)
    ) {
        return Err(DbError::storage(
            SqlState::DatatypeMismatch,
            "physical varlena value supplied for a fixed-width column",
        ));
    }

    match physical {
        VarlenaPhysical::Plain(raw) => {
            if data_type == DataType::Text && std::str::from_utf8(raw).is_err() {
                return Err(corrupt_row("text value is not valid UTF-8"));
            }
            if let DataType::Array(element) = &data_type {
                let array = decode_array_payload(raw)?;
                if array.element_type() != element.element_type() {
                    return Err(corrupt_row(
                        "array payload element type does not match column",
                    ));
                }
            }
            put_varlena(bytes, TAG_PLAIN, raw)
        }
        VarlenaPhysical::Compressed {
            codec,
            dict_id,
            raw_len,
            raw_crc32,
            payload,
        } => {
            validate_inline_compressed_metadata(*codec, *dict_id, *raw_len)?;
            let stored_len = 1usize
                .checked_add(4)
                .and_then(|len| len.checked_add(4))
                .and_then(|len| len.checked_add(4))
                .and_then(|len| len.checked_add(payload.len()))
                .ok_or_else(|| corrupt_row("inline compressed varlena length overflow"))?;
            let mut stored = Vec::with_capacity(stored_len);
            stored.push(*codec);
            stored.extend_from_slice(&dict_id.unwrap_or(0).to_le_bytes());
            stored.extend_from_slice(&raw_len.to_le_bytes());
            stored.extend_from_slice(&raw_crc32.to_le_bytes());
            stored.extend_from_slice(payload);
            put_varlena(bytes, TAG_COMPRESSED, &stored)
        }
        VarlenaPhysical::External(pointer) => {
            if !has_toast_relation {
                return Err(corrupt_row(
                    "external toast value requires a hidden TOAST relation",
                ));
            }
            let pointer = pointer.encode()?;
            put_varlena(bytes, TAG_EXTERNAL, &pointer)
        }
    }
}

fn decode_varlena_physical(
    bytes: &[u8],
    offset: &mut usize,
    version: u8,
    column: usize,
    data_type: DataType,
    has_toast_relation: bool,
) -> Result<DecodedPhysicalValue> {
    let word = read_u32(bytes, offset)?;
    let (tag, stored_len) = if version == ROW_FORMAT_VERSION_V3 {
        unpack_varlena_len(word)?
    } else {
        (TAG_PLAIN, decoded_usize(word, "varlena value length")?)
    };
    let stored = read_exact(bytes, offset, stored_len)?;

    match tag {
        TAG_PLAIN => match data_type {
            DataType::Text => {
                let text = String::from_utf8(stored.to_vec())
                    .map_err(|_| corrupt_row("text value is not valid UTF-8"))?;
                Ok(DecodedPhysicalValue::Value(Value::Text(text)))
            }
            DataType::Bytea => Ok(DecodedPhysicalValue::Value(Value::Bytes(stored.to_vec()))),
            DataType::Array(element) => {
                let array = decode_array_payload(stored)?;
                if array.element_type() != element.element_type() {
                    return Err(corrupt_row(
                        "array payload element type does not match column",
                    ));
                }
                Ok(DecodedPhysicalValue::Value(Value::Array(array)))
            }
            _ => Err(corrupt_row("plain varlena decoded for fixed-width column")),
        },
        TAG_COMPRESSED => decode_inline_compressed(column, stored),
        TAG_EXTERNAL => {
            if !has_toast_relation {
                return Err(corrupt_row(
                    "external toast value requires a hidden TOAST relation",
                ));
            }
            if stored_len != TOAST_POINTER_LEN {
                return Err(corrupt_row(format!(
                    "external toast pointer has {stored_len} bytes, expected {TOAST_POINTER_LEN}"
                )));
            }
            Ok(DecodedPhysicalValue::External {
                column,
                pointer: ToastPointer::decode(stored)?,
            })
        }
        _ => Err(corrupt_row("reserved varlena tag")),
    }
}

fn decode_inline_compressed(column: usize, stored: &[u8]) -> Result<DecodedPhysicalValue> {
    let mut offset = 0;
    let codec = read_u8(stored, &mut offset)?;
    let dict_id_raw = read_u32(stored, &mut offset)?;
    let dict_id = (dict_id_raw != 0).then_some(dict_id_raw);
    let raw_len = read_u32(stored, &mut offset)?;
    let raw_crc32 = read_u32(stored, &mut offset)?;
    validate_decoded_inline_compressed_metadata(codec, dict_id, raw_len)?;
    let payload_len = stored.len() - offset;
    let payload = read_exact(stored, &mut offset, payload_len)?.to_vec();
    Ok(DecodedPhysicalValue::Compressed {
        column,
        codec,
        dict_id,
        raw_len,
        raw_crc32,
        payload,
    })
}

fn validate_inline_compressed_metadata(
    codec: u8,
    dict_id: Option<u32>,
    raw_len: u32,
) -> Result<()> {
    validate_varlena_u32_len(raw_len, "inline compressed raw length")?;
    validate_inline_compressed_codec_metadata(codec, dict_id)
}

fn validate_decoded_inline_compressed_metadata(
    codec: u8,
    dict_id: Option<u32>,
    raw_len: u32,
) -> Result<()> {
    validate_decoded_varlena_u32_len(raw_len, "inline compressed raw length")?;
    validate_inline_compressed_codec_metadata(codec, dict_id)
}

fn validate_inline_compressed_codec_metadata(codec: u8, dict_id: Option<u32>) -> Result<()> {
    match (codec, dict_id) {
        (compress::CODEC_ZSTD, None) => Ok(()),
        (compress::CODEC_ZSTD_DICT, Some(dict_id)) if dict_id != 0 => Ok(()),
        (compress::CODEC_ZSTD_DICT, Some(_)) => Err(corrupt_row(
            "dictionary id 0 is invalid for zstd-dict value",
        )),
        (compress::CODEC_NONE, _) => {
            Err(corrupt_row("codec none is invalid for inline compression"))
        }
        (compress::CODEC_ZSTD, Some(_)) => {
            Err(corrupt_row("dict id is invalid for dict-less zstd value"))
        }
        (compress::CODEC_ZSTD_DICT, None) => {
            Err(corrupt_row("missing dictionary id for zstd-dict value"))
        }
        (other, _) => Err(corrupt_row(format!(
            "unknown inline compressed codec {other}"
        ))),
    }
}

#[allow(
    dead_code,
    reason = "called by encode_row_v3_prepared once the TOAST write path is wired in"
)]
fn put_varlena(bytes: &mut Vec<u8>, tag: u8, payload: &[u8]) -> Result<()> {
    let word = pack_varlena_len(tag, payload.len())?;
    bytes.extend_from_slice(&word.to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(())
}

#[allow(
    dead_code,
    reason = "called by encode_row_v3_prepared once the TOAST write path is wired in"
)]
pub(crate) fn pack_varlena_len(tag: u8, stored_len: usize) -> Result<u32> {
    if tag > TAG_EXTERNAL {
        return Err(corrupt_row(format!("invalid varlena tag {tag}")));
    }
    let stored_len = u32::try_from(stored_len).map_err(|_| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "varlena value exceeds the supported length",
        )
    })?;
    validate_varlena_u32_len(stored_len, "varlena value length")?;
    Ok(((tag as u32) << VARLENA_TAG_SHIFT) | stored_len)
}

pub(crate) fn unpack_varlena_len(word: u32) -> Result<(u8, usize)> {
    let tag = u8::try_from(word >> VARLENA_TAG_SHIFT)
        .map_err(|_| corrupt_row("varlena tag does not fit u8"))?;
    if tag > TAG_EXTERNAL {
        return Err(corrupt_row("reserved varlena tag"));
    }
    Ok((
        tag,
        decoded_usize(word & VARLENA_LEN_MASK, "varlena value length")?,
    ))
}

fn validate_varlena_u32_len(len: u32, what: &str) -> Result<()> {
    if len > VARLENA_LEN_MASK {
        return Err(DbError::storage(
            SqlState::ProgramLimitExceeded,
            format!("{what} exceeds the supported length"),
        ));
    }
    Ok(())
}

fn validate_decoded_varlena_u32_len(len: u32, what: &str) -> Result<()> {
    if len > VARLENA_LEN_MASK {
        return Err(corrupt_row(format!("{what} exceeds the supported length")));
    }
    Ok(())
}

fn validate_toast_pointer_value_id(value_id: u64) -> Result<()> {
    if value_id == 0 || value_id > i64::MAX as u64 {
        return Err(corrupt_row(format!(
            "toast pointer has invalid value_id {value_id}"
        )));
    }
    Ok(())
}

fn validate_toast_pointer_codec(codec: u8) -> Result<()> {
    if !matches!(
        codec,
        compress::CODEC_NONE | compress::CODEC_ZSTD | compress::CODEC_ZSTD_DICT
    ) {
        return Err(corrupt_row(format!(
            "toast pointer uses unsupported codec {codec}"
        )));
    }
    Ok(())
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
        Some(&ROW_FORMAT_VERSION_V2) | Some(&ROW_FORMAT_VERSION_V3) => {
            let header = tuple
                .get(1..1 + V2_MVCC_HEADER_LEN)
                .ok_or_else(|| corrupt_row("tuple is shorter than its v2 header"))?;
            let header = read_mvcc_header(header)?;
            Ok((header.xmin, header.xmax, header.t_ctid, header.infomask))
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
fn write_v2_header(header: &mut [u8], txn_id: TxnId) -> Result<()> {
    write_mvcc_header(header, &MvccHeader::fresh(txn_id, 0))
}

fn write_mvcc_header(header: &mut [u8], mvcc: &MvccHeader) -> Result<()> {
    if header.len() != V2_MVCC_HEADER_LEN {
        return Err(corrupt_row("MVCC header has an invalid length"));
    }
    header[0..2].copy_from_slice(&mvcc.infomask.to_le_bytes());
    header[2..10].copy_from_slice(&mvcc.xmin.to_le_bytes());
    header[10..18].copy_from_slice(&mvcc.xmax.to_le_bytes());
    let (page, slot) = mvcc.t_ctid;
    header[18..22].copy_from_slice(&page.to_le_bytes());
    header[22..24].copy_from_slice(&slot.to_le_bytes());
    Ok(())
}

fn read_mvcc_header(header: &[u8]) -> Result<MvccHeader> {
    if header.len() != V2_MVCC_HEADER_LEN {
        return Err(corrupt_row("row v2 header has the wrong length"));
    }
    let mut offset = 0;
    let infomask = u16::from_le_bytes(read_array(header, &mut offset)?);
    let xmin = u64::from_le_bytes(read_array(header, &mut offset)?);
    let xmax = u64::from_le_bytes(read_array(header, &mut offset)?);
    let page = u32::from_le_bytes(read_array(header, &mut offset)?);
    let slot = u16::from_le_bytes(read_array(header, &mut offset)?);
    Ok(MvccHeader {
        xmin,
        xmax,
        t_ctid: (page, slot),
        infomask,
    })
}

/// Mutate the in-place MVCC header fields of an existing v2/v3 tuple, overwriting
/// `xmax`, `t_ctid`, and `infomask` in `tuple` (the full tuple byte buffer,
/// version byte included). `xmin` is the immutable creator and is left untouched.
///
/// These three are fixed-width header fields, so the tuple length is unchanged —
/// the heap page can rewrite them without relocating the tuple or compacting the
/// page. This is the single codec chokepoint for header-field offsets, so
/// `page.rs` mutates a tuple header through here rather than duplicating layout.
///
/// Returns `InternalError` if the buffer is not an MVCC tuple or is shorter than
/// the MVCC header, so misuse surfaces as a structured `DbError` instead of a
/// panic.
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
    if tuple.is_empty() || (tuple[0] != ROW_FORMAT_VERSION_V2 && tuple[0] != ROW_FORMAT_VERSION_V3)
    {
        return Err(corrupt_row(
            "cannot mutate header of a non-MVCC (or empty) tuple",
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

pub(crate) fn null_bitmap_len(columns: usize) -> usize {
    columns.div_ceil(8)
}

fn set_null(bitmap: &mut [u8], index: usize) -> Result<()> {
    let byte = bitmap
        .get_mut(index / 8)
        .ok_or_else(|| corrupt_row("null bitmap index is outside the bitmap"))?;
    *byte |= 1 << (index % 8);
    Ok(())
}

fn is_null(bitmap: &[u8], index: usize) -> Result<bool> {
    let byte = bitmap
        .get(index / 8)
        .ok_or_else(|| corrupt_row("null bitmap index is outside the bitmap"))?;
    Ok(*byte & (1 << (index % 8)) != 0)
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let mut reader = CheckedSliceReader::at(bytes, *offset)
        .map_err(|err| corrupt_row(format!("invalid row offset: {err}")))?;
    let raw = reader
        .take(len)
        .map_err(|err| corrupt_row(format!("row ended unexpectedly: {err}")))?;
    *offset = reader.position();
    Ok(raw)
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    read_exact(bytes, offset, N)?.try_into().map_err(|_| {
        corrupt_row(format!(
            "fixed-width decoder expected {N} bytes after bounds validation"
        ))
    })
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(bytes, offset)?))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8> {
    let [value] = read_array::<1>(bytes, offset)?;
    Ok(value)
}

fn decoded_usize(value: u32, what: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| corrupt_row(format!("{what} does not fit usize")))
}

fn fallible_decode_vec<T>(capacity: usize, what: &str) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(|_| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            format!("cannot allocate {what}"),
        )
    })?;
    Ok(values)
}

fn corrupt_row(message: impl Into<String>) -> common::DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use common::{
        ArrayDimension, ArrayType, ColumnDef, CompressionSetting, DataType, FROZEN_XID,
        INVALID_XID, Key, RelationKind, Row, SqlArray, SqlState, TableSchema, ToastOptions, Value,
        XMAX_COMMITTED, XMIN_COMMITTED,
    };

    use super::{
        DecodedPhysicalValue, INVALID_TID, MAX_ARRAY_PAYLOAD_BYTES, MvccHeader,
        PreparedColumnValue, ROW_FORMAT_VERSION, ROW_FORMAT_VERSION_V1, ROW_FORMAT_VERSION_V3,
        TAG_COMPRESSED, TAG_EXTERNAL, TAG_PLAIN, TOAST_POINTER_LEN, ToastPointer,
        V2_MVCC_HEADER_LEN, VARLENA_LEN_MASK, VarlenaPhysical, decode_array_payload,
        decode_inline_compressed, decode_key, decode_physical_row, decode_row,
        encode_array_payload, encode_key, encode_row, encode_row_v3_prepared, null_bitmap_len,
        pack_varlena_len, put_interval, put_numeric, set_mvcc_header_fields, unpack_varlena_len,
        validate_decoded_array_payload_len,
    };

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 1,
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    object_id: 1,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    object_id: 2,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
            next_column_object_id: u32::MAX,
        }
    }

    fn two_text_schema() -> TableSchema {
        let mut schema = schema();
        schema.columns.push(ColumnDef {
            id: 2,
            object_id: 3,
            name: "note2".to_string(),
            data_type: DataType::Text,
            nullable: true,
            max_length: None,
            default: None,
            pg_type: None,
        });
        schema
    }

    fn schema_with_toast_relation() -> TableSchema {
        let mut schema = schema();
        schema.toast_table_id = Some(2);
        schema
    }

    fn array_schema() -> TableSchema {
        let mut schema = schema_with_toast_relation();
        schema.columns[1].data_type = DataType::Array(ArrayType::new(DataType::Integer).unwrap());
        schema
    }

    fn integer_array() -> SqlArray {
        SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(2, -1), ArrayDimension::new(2, 3)],
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(3),
                Value::Integer(4),
            ],
        )
        .unwrap()
    }

    fn prepared_from_row(row: &Row) -> Vec<PreparedColumnValue> {
        row.values
            .iter()
            .cloned()
            .map(|value| match value {
                Value::Null => PreparedColumnValue::Null,
                other => PreparedColumnValue::Value(other),
            })
            .collect()
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
                Value::Time(value) => bytes.extend_from_slice(&value.to_le_bytes()),
                Value::TimestampTz(value) => bytes.extend_from_slice(&value.to_le_bytes()),
                Value::Interval(value) => put_interval(&mut bytes, value),
                Value::Bytes(value) => {
                    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(value);
                }
                Value::Uuid(value) => bytes.extend_from_slice(value),
                Value::Float(value) => bytes.extend_from_slice(&value.0.to_le_bytes()),
                Value::Numeric(value) => put_numeric(&mut bytes, value),
                Value::Real(value) => bytes.extend_from_slice(&value.0.to_le_bytes()),
                Value::Array(_) => panic!("legacy test encoder does not support arrays"),
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
    fn varlena_tagged_length_words_round_trip() {
        for tag in [TAG_PLAIN, TAG_COMPRESSED, TAG_EXTERNAL] {
            let word = pack_varlena_len(tag, 123).unwrap();
            assert_eq!(unpack_varlena_len(word).unwrap(), (tag, 123));
        }
    }

    #[test]
    fn reserved_varlena_tag_is_corruption() {
        let word = 3u32 << 30;
        let err = unpack_varlena_len(word).unwrap_err();
        assert!(err.message.contains("reserved varlena tag"));
    }

    #[test]
    fn v3_row_with_reserved_varlena_tag_is_corruption() {
        let row = Row {
            values: vec![Value::Integer(42), Value::Text("plain".to_string())],
        };
        let mut bytes = encode_row_v3_prepared(
            &schema(),
            &MvccHeader::fresh(7, 0),
            &prepared_from_row(&row),
        )
        .unwrap();
        let text_len_offset = 1 + V2_MVCC_HEADER_LEN + null_bitmap_len(schema().columns.len()) + 8;
        bytes[text_len_offset..text_len_offset + 4].copy_from_slice(&(3u32 << 30).to_le_bytes());

        let err = decode_physical_row(&schema(), &bytes).unwrap_err();
        assert!(err.message.contains("reserved varlena tag"));
    }

    #[test]
    fn oversized_varlena_length_is_rejected() {
        let err = pack_varlena_len(TAG_PLAIN, VARLENA_LEN_MASK as usize + 1).unwrap_err();
        assert_eq!(err.code, common::SqlState::ProgramLimitExceeded);
    }

    #[test]
    fn v3_plain_row_round_trips() {
        let row = Row {
            values: vec![Value::Integer(42), Value::Text("plain".to_string())],
        };
        let values = vec![
            PreparedColumnValue::Value(Value::Integer(42)),
            PreparedColumnValue::Varlena(VarlenaPhysical::Plain(b"plain".to_vec())),
        ];
        let bytes = encode_row_v3_prepared(&schema(), &MvccHeader::fresh(7, 0), &values).unwrap();

        assert_eq!(bytes[0], ROW_FORMAT_VERSION_V3);
        let decoded = decode_row(&schema(), &bytes).unwrap();
        assert_eq!(decoded.row, row);
        assert_eq!(decoded.xmin, 7);
        assert_eq!(decoded.xmax, INVALID_XID);
    }

    #[test]
    fn v3_plain_varlena_length_word_matches_v2() {
        let row = Row {
            values: vec![Value::Integer(42), Value::Text("same bytes".to_string())],
        };
        let mut v2 = encode_row(&schema(), &row, 7).unwrap();
        let v3 = encode_row_v3_prepared(
            &schema(),
            &MvccHeader::fresh(7, 0),
            &prepared_from_row(&row),
        )
        .unwrap();
        v2[0] = ROW_FORMAT_VERSION_V3;

        assert_eq!(v3, v2);
    }

    #[test]
    fn v3_plain_two_text_columns_adds_no_per_column_overhead() {
        let schema = two_text_schema();
        let row = Row {
            values: vec![
                Value::Integer(42),
                Value::Text("first".to_string()),
                Value::Text("second".to_string()),
            ],
        };
        let v2 = encode_row(&schema, &row, 7).unwrap();
        let v3 =
            encode_row_v3_prepared(&schema, &MvccHeader::fresh(7, 0), &prepared_from_row(&row))
                .unwrap();

        assert_eq!(v3.len(), v2.len());
    }

    #[test]
    fn v3_inline_compressed_metadata_decodes_physically() {
        let raw = b"hello compressed";
        let payload = b"zstd bytes".to_vec();
        let raw_crc32 = crc32fast::hash(raw);
        let values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::Compressed {
                codec: compress::CODEC_ZSTD,
                dict_id: None,
                raw_len: raw.len() as u32,
                raw_crc32,
                payload: payload.clone(),
            }),
        ];
        let bytes = encode_row_v3_prepared(&schema(), &MvccHeader::fresh(7, 0), &values).unwrap();
        let decoded = decode_physical_row(&schema(), &bytes).unwrap();

        assert_eq!(decoded.header, MvccHeader::fresh(7, 0));
        assert_eq!(
            decoded.values[1],
            DecodedPhysicalValue::Compressed {
                column: 1,
                codec: compress::CODEC_ZSTD,
                dict_id: None,
                raw_len: raw.len() as u32,
                raw_crc32,
                payload,
            }
        );
    }

    #[test]
    fn v3_inline_zstd_dict_metadata_preserves_dict_id_and_crc() {
        let raw = b"dictionary compressed";
        let payload = b"dict payload".to_vec();
        let raw_crc32 = crc32fast::hash(raw);
        let values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::Compressed {
                codec: compress::CODEC_ZSTD_DICT,
                dict_id: Some(9),
                raw_len: raw.len() as u32,
                raw_crc32,
                payload: payload.clone(),
            }),
        ];
        let bytes = encode_row_v3_prepared(&schema(), &MvccHeader::fresh(7, 0), &values).unwrap();
        let decoded = decode_physical_row(&schema(), &bytes).unwrap();

        assert_eq!(
            decoded.values[1],
            DecodedPhysicalValue::Compressed {
                column: 1,
                codec: compress::CODEC_ZSTD_DICT,
                dict_id: Some(9),
                raw_len: raw.len() as u32,
                raw_crc32,
                payload,
            }
        );
    }

    #[test]
    fn v3_inline_zstd_dict_rejects_zero_dict_id() {
        let values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::Compressed {
                codec: compress::CODEC_ZSTD_DICT,
                dict_id: Some(0),
                raw_len: 4,
                raw_crc32: 0,
                payload: b"dict payload".to_vec(),
            }),
        ];

        let err = encode_row_v3_prepared(&schema(), &MvccHeader::fresh(7, 0), &values).unwrap_err();
        assert!(err.message.contains("dictionary id 0"));
    }

    #[test]
    fn v3_external_pointer_is_exactly_seventeen_bytes() {
        let schema = schema_with_toast_relation();
        let pointer = ToastPointer {
            value_id: 99,
            raw_len: 1234,
            stored_len: 567,
            codec: compress::CODEC_ZSTD,
        };
        let encoded = pointer.encode().unwrap();
        assert_eq!(encoded.len(), TOAST_POINTER_LEN);
        assert_eq!(ToastPointer::decode(&encoded).unwrap(), pointer);

        let values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::External(pointer.clone())),
        ];
        let bytes = encode_row_v3_prepared(&schema, &MvccHeader::fresh(7, 0), &values).unwrap();
        let decoded = decode_physical_row(&schema, &bytes).unwrap();
        assert_eq!(
            decoded.values[1],
            DecodedPhysicalValue::External { column: 1, pointer }
        );
    }

    #[test]
    fn toast_pointer_accepts_dict_codec_with_dict_id_in_stream_header() {
        let pointer = ToastPointer {
            value_id: 99,
            raw_len: 1234,
            stored_len: 567,
            codec: compress::CODEC_ZSTD_DICT,
        };

        let encoded = pointer.encode().unwrap();
        assert_eq!(ToastPointer::decode(&encoded).unwrap(), pointer);
    }

    #[test]
    fn v3_external_pointer_requires_hidden_toast_relation() {
        let pointer = ToastPointer {
            value_id: 99,
            raw_len: 1234,
            stored_len: 567,
            codec: compress::CODEC_ZSTD,
        };
        let values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::External(pointer)),
        ];

        let err = encode_row_v3_prepared(&schema(), &MvccHeader::fresh(7, 0), &values).unwrap_err();
        assert!(err.message.contains("requires a hidden TOAST relation"));

        let toast_schema = schema_with_toast_relation();
        let bytes =
            encode_row_v3_prepared(&toast_schema, &MvccHeader::fresh(7, 0), &values).unwrap();
        let err = decode_physical_row(&schema(), &bytes).unwrap_err();
        assert!(err.message.contains("requires a hidden TOAST relation"));
    }

    #[test]
    fn toast_pointer_rejects_value_id_outside_hidden_key_range() {
        for value_id in [0, i64::MAX as u64 + 1] {
            let pointer = ToastPointer {
                value_id,
                raw_len: 1234,
                stored_len: 567,
                codec: compress::CODEC_ZSTD,
            };

            let err = pointer.encode().unwrap_err();
            assert!(err.message.contains("invalid value_id"));
        }
    }

    #[test]
    fn toast_pointer_decode_length_violations_are_corruption() {
        let pointer = ToastPointer {
            value_id: 99,
            raw_len: 1234,
            stored_len: 567,
            codec: compress::CODEC_ZSTD,
        };
        let mut encoded = pointer.encode().unwrap();
        encoded[8..12].copy_from_slice(&(VARLENA_LEN_MASK + 1).to_le_bytes());

        let err = ToastPointer::decode(&encoded).unwrap_err();
        assert_eq!(err.code, common::SqlState::InternalError);
        assert!(err.message.contains("exceeds the supported length"));
    }

    #[test]
    fn inline_compressed_decode_length_violations_are_corruption() {
        let mut stored = Vec::new();
        stored.push(compress::CODEC_ZSTD);
        stored.extend_from_slice(&0u32.to_le_bytes());
        stored.extend_from_slice(&(VARLENA_LEN_MASK + 1).to_le_bytes());
        stored.extend_from_slice(&0u32.to_le_bytes());
        stored.extend_from_slice(b"payload");

        let err = decode_inline_compressed(1, &stored).unwrap_err();
        assert_eq!(err.code, common::SqlState::InternalError);
        assert!(err.message.contains("exceeds the supported length"));
    }

    #[test]
    fn public_decode_row_rejects_v3_physical_toast_values_until_detoast_lands() {
        let compressed_values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::Compressed {
                codec: compress::CODEC_ZSTD,
                dict_id: None,
                raw_len: 4,
                raw_crc32: crc32fast::hash(b"test"),
                payload: b"payload".to_vec(),
            }),
        ];
        let compressed =
            encode_row_v3_prepared(&schema(), &MvccHeader::fresh(7, 0), &compressed_values)
                .unwrap();

        let err = decode_row(&schema(), &compressed).unwrap_err();
        assert!(err.message.contains("inline compressed TOAST value"));

        let schema = schema_with_toast_relation();
        let external_values = vec![
            PreparedColumnValue::Value(Value::Integer(1)),
            PreparedColumnValue::Varlena(VarlenaPhysical::External(ToastPointer {
                value_id: 99,
                raw_len: 4,
                stored_len: 8,
                codec: compress::CODEC_ZSTD,
            })),
        ];
        let external =
            encode_row_v3_prepared(&schema, &MvccHeader::fresh(7, 0), &external_values).unwrap();

        let err = decode_row(&schema, &external).unwrap_err();
        assert!(err.message.contains("external TOAST value"));
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
    fn set_mvcc_header_fields_accepts_v3_tuple() {
        let row = Row {
            values: vec![Value::Integer(42), Value::Text("keep".to_string())],
        };
        let mut bytes = encode_row_v3_prepared(
            &schema(),
            &MvccHeader::fresh(7, 0),
            &prepared_from_row(&row),
        )
        .unwrap();

        set_mvcc_header_fields(&mut bytes, 99, (4, 5), XMAX_COMMITTED).unwrap();
        let decoded = decode_row(&schema(), &bytes).unwrap();

        assert_eq!(decoded.xmax, 99);
        assert_eq!(decoded.t_ctid, (4, 5));
        assert_eq!(decoded.infomask, XMAX_COMMITTED);
        assert_eq!(decoded.row, row);
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
    fn array_payload_and_key_round_trip_shape_nulls_and_bounds() {
        let array = integer_array();
        let payload = encode_array_payload(&array).unwrap();
        assert_eq!(decode_array_payload(&payload).unwrap(), array);

        let key = Key(vec![Value::Array(array)]);
        assert_eq!(decode_key(&encode_key(&key).unwrap()).unwrap(), key);

        let oversized = SqlArray::new(
            DataType::Text,
            vec![ArrayDimension::new(1, 1)],
            vec![Value::Text("x".repeat(buffer::PAGE_SIZE))],
        )
        .unwrap();
        let error = encode_key(&Key(vec![Value::Array(oversized)])).unwrap_err();
        assert_eq!(error.code, SqlState::ProgramLimitExceeded);
    }

    #[test]
    fn array_rows_round_trip_in_legacy_and_v3_formats() {
        let row = Row {
            values: vec![Value::Integer(7), Value::Array(integer_array())],
        };
        let legacy = encode_row(&array_schema(), &row, 9).unwrap();
        assert_eq!(decode_row(&array_schema(), &legacy).unwrap().row, row);

        let prepared = prepared_from_row(&row);
        let v3 =
            encode_row_v3_prepared(&array_schema(), &MvccHeader::fresh(9, 0), &prepared).unwrap();
        assert_eq!(decode_row(&array_schema(), &v3).unwrap().row, row);
    }

    #[test]
    fn array_payload_rejects_unknown_version_truncation_and_trailing_bytes() {
        let payload = encode_array_payload(&integer_array()).unwrap();
        let mut unknown = payload.clone();
        unknown[0] += 1;
        assert!(decode_array_payload(&unknown).is_err());
        assert!(decode_array_payload(&payload[..payload.len() - 1]).is_err());
        let mut trailing = payload;
        trailing.push(0);
        assert!(decode_array_payload(&trailing).is_err());

        let mut bad_cardinality = encode_array_payload(&integer_array()).unwrap();
        bad_cardinality[3..7].copy_from_slice(&5_u32.to_le_bytes());
        assert!(decode_array_payload(&bad_cardinality).is_err());

        let mut allocation_bomb = vec![1, 0, 1];
        allocation_bomb.extend_from_slice(
            &u32::try_from(common::MAX_ARRAY_ELEMENTS + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert!(decode_array_payload(&allocation_bomb).is_err());
        assert!(validate_decoded_array_payload_len(MAX_ARRAY_PAYLOAD_BYTES + 1).is_err());

        let mut bad_numeric = vec![1, 12];
        bad_numeric.extend_from_slice(&0_u32.to_le_bytes());
        bad_numeric.extend_from_slice(&1_u32.to_le_bytes());
        bad_numeric.extend_from_slice(&[0, 0, 0, 0, 0]);
        assert!(decode_array_payload(&bad_numeric).is_err());
    }

    #[test]
    fn decode_rejects_unknown_row_format_version() {
        let row = Row {
            values: vec![Value::Integer(7), Value::Null],
        };
        let mut bytes = encode_row(&schema(), &row, 1).unwrap();
        bytes[0] = ROW_FORMAT_VERSION_V3 + 1;

        let err = decode_row(&schema(), &bytes).unwrap_err();
        assert!(err.message.contains("row format version"));
    }
}
