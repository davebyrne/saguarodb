#![cfg_attr(
    not(test),
    deny(
        clippy::arithmetic_side_effects,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::indexing_slicing
    )
)]

use common::{CheckedSliceReader, DbError, Result, SqlState};

/// Codec ids shared by page/WAL compression and TOAST value payloads
/// (`compression.md` §3). `CODEC_NONE` is valid only for value payloads;
/// page-at-rest envelopes and compressed WAL FPIs must use a real codec.
pub const CODEC_NONE: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;
pub const CODEC_ZSTD_DICT: u8 = 2;

/// zstd levels: at-rest and TOAST value compression run off the hottest DML
/// path, while WAL FPI compression runs inline on the DML path.
pub const LEVEL_AT_REST: i32 = 3;
pub const TOAST_ZSTD_LEVEL: i32 = LEVEL_AT_REST;
pub const LEVEL_WAL: i32 = 1;

/// First 6 bytes of a compressed page slot. Bytes 4 and 5 sit where a raw v2
/// page stores PageType/PageVersion; 0xFF is invalid for both, so no valid
/// raw page can collide with the marker (`compression.md` §5).
pub const ENVELOPE_MARKER: [u8; 6] = [b'S', b'G', b'C', b'P', 0xFF, 0xFF];
pub const ENVELOPE_VERSION: u8 = 1;
/// marker(6) + version(1) + codec(1) + dict_id(4) + payload_len(2) + crc32(4)
pub const ENVELOPE_HEADER_LEN: usize = 18;

#[derive(Debug)]
pub struct Envelope<'a> {
    pub codec: u8,
    pub dict_id: u32,
    pub payload: &'a [u8],
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

fn validate_envelope_codec(codec: u8) -> Result<()> {
    if codec != CODEC_ZSTD && codec != CODEC_ZSTD_DICT {
        return Err(corrupt(format!("unknown envelope codec {codec}")));
    }
    Ok(())
}

pub fn is_envelope(slot: &[u8]) -> bool {
    slot.starts_with(&ENVELOPE_MARKER)
}

pub fn encode_envelope(codec: u8, dict_id: u32, payload: &[u8]) -> Result<Vec<u8>> {
    validate_envelope_codec(codec)?;
    let payload_len = u16::try_from(payload.len())
        .map_err(|_| corrupt(format!("envelope payload too large: {}", payload.len())))?;
    let envelope_len = ENVELOPE_HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| corrupt("compressed page envelope length overflows"))?;
    let mut out = Vec::new();
    out.try_reserve_exact(envelope_len)
        .map_err(|_| corrupt("cannot allocate compressed page envelope"))?;
    out.extend_from_slice(&ENVELOPE_MARKER);
    out.push(ENVELOPE_VERSION);
    out.push(codec);
    out.extend_from_slice(&dict_id.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Compress a single TOAST value payload with dict-less zstd.
///
/// The caller owns all policy decisions: this helper always returns the
/// compressed bytes if zstd succeeds, even when the output is larger than the
/// raw input.
pub fn compress_value_zstd(raw: &[u8]) -> Result<Vec<u8>> {
    zstd::bulk::compress(raw, TOAST_ZSTD_LEVEL)
        .map_err(|err| corrupt(format!("zstd value compress failed: {err}")))
}

pub(crate) fn decompress_value_zstd(payload: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    let value = zstd::bulk::decompress(payload, raw_len)
        .map_err(|err| corrupt(format!("zstd value decompress failed: {err}")))?;
    ensure_value_len(&value, raw_len)?;
    Ok(value)
}

pub(crate) fn ensure_value_len(value: &[u8], raw_len: usize) -> Result<()> {
    if value.len() != raw_len {
        return Err(corrupt(format!(
            "decompressed value is {} bytes, expected {raw_len}",
            value.len()
        )));
    }
    Ok(())
}

pub fn decode_envelope(slot: &[u8]) -> Result<Envelope<'_>> {
    if !is_envelope(slot) {
        return Err(corrupt("not a compressed page envelope"));
    }
    if slot.len() < ENVELOPE_HEADER_LEN {
        return Err(corrupt("compressed page envelope truncated"));
    }
    let mut reader = CheckedSliceReader::new(slot);
    reader
        .take(ENVELOPE_MARKER.len())
        .map_err(|_| corrupt("compressed page envelope truncated"))?;
    let version = reader
        .read_u8()
        .map_err(|_| corrupt("compressed page envelope version is truncated"))?;
    if version != ENVELOPE_VERSION {
        return Err(corrupt(format!("unknown envelope version {version}")));
    }
    let codec = reader
        .read_u8()
        .map_err(|_| corrupt("compressed page envelope codec is truncated"))?;
    validate_envelope_codec(codec)?;
    let dict_id = reader
        .read_u32_le()
        .map_err(|_| corrupt("compressed page dictionary id is truncated"))?;
    let payload_len = usize::from(
        reader
            .read_u16_le()
            .map_err(|_| corrupt("compressed page payload length is truncated"))?,
    );
    let stored_crc = reader
        .read_u32_le()
        .map_err(|_| corrupt("compressed page CRC is truncated"))?;
    let payload = reader
        .take(payload_len)
        .map_err(|_| corrupt("envelope payload extends past the page slot"))?;
    if crc32fast::hash(payload) != stored_crc {
        return Err(corrupt("compressed page envelope CRC mismatch"));
    }
    Ok(Envelope {
        codec,
        dict_id,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips() {
        let payload = b"compressed bytes".to_vec();
        let env = encode_envelope(CODEC_ZSTD_DICT, 7, &payload).unwrap();
        assert_eq!(env.len(), ENVELOPE_HEADER_LEN + payload.len());
        assert!(is_envelope(&env));
        let decoded = decode_envelope(&env).unwrap();
        assert_eq!(decoded.codec, CODEC_ZSTD_DICT);
        assert_eq!(decoded.dict_id, 7);
        assert_eq!(decoded.payload, payload.as_slice());
    }

    #[test]
    fn raw_page_and_zeroed_slot_are_not_envelopes() {
        // A valid v2 page has PageVersion = 2 at offset 5; envelope marker
        // requires 0xFF there, so no valid page collides.
        let mut raw = vec![0u8; 8192];
        raw[4] = 1; // PageType heap
        raw[5] = 2; // PageVersion 2
        assert!(!is_envelope(&raw));
        assert!(!is_envelope(&vec![0u8; 8192]));
        assert!(!is_envelope(b"SG")); // shorter than the marker
    }

    #[test]
    fn decode_rejects_crc_and_version_tampering() {
        let env = encode_envelope(CODEC_ZSTD, 0, b"payload").unwrap();

        let mut bad_crc = env.clone();
        *bad_crc.last_mut().unwrap() ^= 0xFF; // flip a payload byte
        let err = decode_envelope(&bad_crc).unwrap_err();
        assert_eq!(err.kind, common::ErrorKind::Storage);
        assert_eq!(err.code, common::SqlState::InternalError);

        let mut bad_version = env.clone();
        bad_version[6] = 99;
        assert!(decode_envelope(&bad_version).is_err());

        let mut bad_codec = env.clone();
        bad_codec[7] = 42;
        assert!(decode_envelope(&bad_codec).is_err());

        let mut no_codec = env.clone();
        no_codec[7] = CODEC_NONE;
        assert!(decode_envelope(&no_codec).is_err());

        let mut truncated = env.clone();
        truncated.truncate(ENVELOPE_HEADER_LEN - 1);
        assert!(decode_envelope(&truncated).is_err());

        // Declared length larger than the slot bytes actually present.
        let mut short_payload = env.clone();
        short_payload.truncate(ENVELOPE_HEADER_LEN + 3);
        assert!(decode_envelope(&short_payload).is_err());
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let too_big = vec![0u8; usize::from(u16::MAX) + 1];
        assert!(encode_envelope(CODEC_ZSTD, 0, &too_big).is_err());
    }

    #[test]
    fn encode_rejects_non_envelope_codecs() {
        assert!(encode_envelope(CODEC_NONE, 0, b"payload").is_err());
        assert!(encode_envelope(99, 0, b"payload").is_err());
    }

    #[test]
    fn zstd_bulk_round_trips_8k_image() {
        // Sanity-check the external zstd API shapes we rely on.
        let image: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let compressed = zstd::bulk::compress(&image, LEVEL_AT_REST).unwrap();
        assert!(compressed.len() < image.len());
        let restored = zstd::bulk::decompress(&compressed, image.len()).unwrap();
        assert_eq!(restored, image);
    }
}
