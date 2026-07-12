use std::io::{Read, Write};

use common::{DbError, ExecRow, Result, Value};
use spill::{RetainedSize, SpillRecord, codec};

pub(super) struct SpillRow {
    pub row: ExecRow,
    pub keys: Vec<Value>,
    pub ordinal: u64,
    pub source: u8,
}

impl RetainedSize for SpillRow {
    fn retained_size(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
            + self
                .row
                .retained_size()
                .saturating_sub(std::mem::size_of::<ExecRow>() as u64)
            + self
                .keys
                .retained_size()
                .saturating_sub(std::mem::size_of::<Vec<Value>>() as u64)
    }
}

impl SpillRecord for SpillRow {
    fn encoded_len(&self) -> Result<u64> {
        codec::exec_row_len(&self.row)?
            .checked_add(codec::values_len(&self.keys)?)
            .and_then(|length| length.checked_add(9))
            .ok_or_else(|| DbError::io("spill row length overflow"))
    }

    fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
        codec::encode_exec_row(&self.row, writer)?;
        codec::encode_values(&self.keys, writer)?;
        writer
            .write_all(&self.ordinal.to_le_bytes())
            .map_err(spill_io_error)?;
        writer.write_all(&[self.source]).map_err(spill_io_error)
    }

    fn decode<R: Read>(reader: &mut R, _payload_len: u64) -> Result<Self> {
        let row = codec::decode_exec_row(reader)?;
        let keys = codec::decode_values(reader)?;
        let mut ordinal = [0; 8];
        reader.read_exact(&mut ordinal).map_err(spill_io_error)?;
        let mut source = [0; 1];
        reader.read_exact(&mut source).map_err(spill_io_error)?;
        Ok(Self {
            row,
            keys,
            ordinal: u64::from_le_bytes(ordinal),
            source: source[0],
        })
    }
}

fn spill_io_error(error: impl std::fmt::Display) -> DbError {
    DbError::io(format!("spill I/O failed: {error}"))
}
