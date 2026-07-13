#![deny(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::indexing_slicing
)]

use std::fmt;

/// The reason a [`CheckedSliceReader`] operation could not be completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SliceReadErrorKind {
    /// The requested starting position is outside the input slice.
    PositionOutOfBounds,
    /// Adding the requested length to the current position overflowed `usize`.
    LengthOverflow,
    /// The input ended before the requested field was complete.
    Truncated,
    /// A decoder required complete consumption but bytes remained.
    TrailingBytes,
}

/// Bounds failure returned by [`CheckedSliceReader`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SliceReadError {
    kind: SliceReadErrorKind,
    position: usize,
    requested: usize,
    remaining: usize,
}

impl SliceReadError {
    /// Classifies the failed operation.
    pub fn kind(self) -> SliceReadErrorKind {
        self.kind
    }

    /// Returns the byte position at which the operation failed.
    pub fn position(self) -> usize {
        self.position
    }

    /// Returns the number of bytes requested by the failed operation.
    pub fn requested(self) -> usize {
        self.requested
    }

    /// Returns the number of bytes available at the failure position.
    pub fn remaining(self) -> usize {
        self.remaining
    }
}

impl fmt::Display for SliceReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            SliceReadErrorKind::PositionOutOfBounds => write!(
                formatter,
                "byte position {} is outside the input",
                self.position
            ),
            SliceReadErrorKind::LengthOverflow => write!(
                formatter,
                "byte range at {} with length {} overflows",
                self.position, self.requested
            ),
            SliceReadErrorKind::Truncated => write!(
                formatter,
                "byte range at {} needs {} bytes but only {} remain",
                self.position, self.requested, self.remaining
            ),
            SliceReadErrorKind::TrailingBytes => write!(
                formatter,
                "{} trailing bytes remain at position {}",
                self.remaining, self.position
            ),
        }
    }
}

impl std::error::Error for SliceReadError {}

/// A forward-only reader that makes offset arithmetic and slice access
/// fallible. Its private position is advanced only after a successful read.
#[derive(Clone, Debug)]
pub struct CheckedSliceReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> CheckedSliceReader<'a> {
    /// Starts reading at the beginning of `bytes`.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Starts reading at `position`, rejecting a position beyond the input.
    pub fn at(bytes: &'a [u8], position: usize) -> Result<Self, SliceReadError> {
        if position > bytes.len() {
            return Err(SliceReadError {
                kind: SliceReadErrorKind::PositionOutOfBounds,
                position,
                requested: 0,
                remaining: 0,
            });
        }
        Ok(Self { bytes, position })
    }

    /// Returns the current byte position.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Returns the unconsumed byte count.
    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    /// Reads exactly `len` bytes and advances only on success.
    pub fn take(&mut self, len: usize) -> Result<&'a [u8], SliceReadError> {
        let remaining = self.remaining();
        let end = self.position.checked_add(len).ok_or(SliceReadError {
            kind: SliceReadErrorKind::LengthOverflow,
            position: self.position,
            requested: len,
            remaining,
        })?;
        let value = self.bytes.get(self.position..end).ok_or(SliceReadError {
            kind: SliceReadErrorKind::Truncated,
            position: self.position,
            requested: len,
            remaining,
        })?;
        self.position = end;
        Ok(value)
    }

    /// Reads the rest of the input.
    pub fn take_remaining(&mut self) -> Result<&'a [u8], SliceReadError> {
        self.take(self.remaining())
    }

    /// Reads one byte.
    pub fn read_u8(&mut self) -> Result<u8, SliceReadError> {
        let [value] = self.read_array::<1>()?;
        Ok(value)
    }

    /// Reads an exact-size byte array.
    pub fn read_array<const N: usize>(&mut self) -> Result<[u8; N], SliceReadError> {
        let position = self.position;
        let bytes = self.take(N)?;
        bytes.try_into().map_err(|_| SliceReadError {
            kind: SliceReadErrorKind::Truncated,
            position,
            requested: N,
            remaining: bytes.len(),
        })
    }

    /// Reads a little-endian `u16`.
    pub fn read_u16_le(&mut self) -> Result<u16, SliceReadError> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    /// Reads a big-endian `i16`.
    pub fn read_i16_be(&mut self) -> Result<i16, SliceReadError> {
        Ok(i16::from_be_bytes(self.read_array()?))
    }

    /// Reads a little-endian `u32`.
    pub fn read_u32_le(&mut self) -> Result<u32, SliceReadError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    /// Reads a big-endian `i32`.
    pub fn read_i32_be(&mut self) -> Result<i32, SliceReadError> {
        Ok(i32::from_be_bytes(self.read_array()?))
    }

    /// Reads a little-endian `u64`.
    pub fn read_u64_le(&mut self) -> Result<u64, SliceReadError> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    /// Requires that the input has been consumed exactly.
    pub fn finish(self) -> Result<(), SliceReadError> {
        let remaining = self.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(SliceReadError {
                kind: SliceReadErrorKind::TrailingBytes,
                position: self.position,
                requested: 0,
                remaining,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckedSliceReader, SliceReadErrorKind};

    #[test]
    fn reads_fixed_width_values_and_tracks_position() {
        let mut reader = CheckedSliceReader::new(&[0x34, 0x12, 0, 0, 0, 7]);
        assert_eq!(reader.read_u16_le().unwrap(), 0x1234);
        assert_eq!(reader.read_i32_be().unwrap(), 7);
        assert_eq!(reader.position(), 6);
        assert_eq!(reader.remaining(), 0);
        reader.finish().unwrap();
    }

    #[test]
    fn failed_reads_do_not_advance() {
        let mut reader = CheckedSliceReader::new(&[1, 2]);
        let err = reader.take(3).unwrap_err();
        assert_eq!(err.kind(), SliceReadErrorKind::Truncated);
        assert_eq!(reader.position(), 0);
        assert_eq!(reader.take(2).unwrap(), &[1, 2]);
    }

    #[test]
    fn rejects_position_and_length_overflow() {
        let bytes = [0];
        let err = CheckedSliceReader::at(&bytes, 2).unwrap_err();
        assert_eq!(err.kind(), SliceReadErrorKind::PositionOutOfBounds);

        let mut reader = CheckedSliceReader::at(&bytes, 1).unwrap();
        let err = reader.take(usize::MAX).unwrap_err();
        assert_eq!(err.kind(), SliceReadErrorKind::LengthOverflow);
    }

    #[test]
    fn finish_rejects_trailing_bytes() {
        let err = CheckedSliceReader::new(&[1]).finish().unwrap_err();
        assert_eq!(err.kind(), SliceReadErrorKind::TrailingBytes);
        assert_eq!(err.remaining(), 1);
    }
}
