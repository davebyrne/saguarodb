//! Compression codecs, the at-rest page envelope, per-table dictionaries, and
//! the shared `CompressionRegistry` (`docs/specs/compression.md`).

mod codec;
mod dict;
mod registry;

pub use codec::{
    CODEC_ZSTD, CODEC_ZSTD_DICT, ENVELOPE_HEADER_LEN, ENVELOPE_MARKER, ENVELOPE_VERSION, Envelope,
    LEVEL_AT_REST, LEVEL_WAL, decode_envelope, encode_envelope, is_envelope,
};
pub use dict::{DictStore, train_dictionary};
pub use registry::{CompressionRegistry, FileCompression};
