use common::{DbError, Result, SqlState};

/// Codec ids stored in envelopes and WAL records (`compression.md` §3).
pub const CODEC_ZSTD: u8 = 1;
pub const CODEC_ZSTD_DICT: u8 = 2;

/// zstd levels: at-rest runs on background flush paths, WAL on the DML path.
pub const LEVEL_AT_REST: i32 = 3;
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

pub fn is_envelope(slot: &[u8]) -> bool {
    slot.len() >= ENVELOPE_MARKER.len() && slot[..ENVELOPE_MARKER.len()] == ENVELOPE_MARKER
}

pub fn encode_envelope(codec: u8, dict_id: u32, payload: &[u8]) -> Result<Vec<u8>> {
    let payload_len = u16::try_from(payload.len())
        .map_err(|_| corrupt(format!("envelope payload too large: {}", payload.len())))?;
    let mut out = Vec::with_capacity(ENVELOPE_HEADER_LEN + payload.len());
    out.extend_from_slice(&ENVELOPE_MARKER);
    out.push(ENVELOPE_VERSION);
    out.push(codec);
    out.extend_from_slice(&dict_id.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_envelope(slot: &[u8]) -> Result<Envelope<'_>> {
    if !is_envelope(slot) {
        return Err(corrupt("not a compressed page envelope"));
    }
    if slot.len() < ENVELOPE_HEADER_LEN {
        return Err(corrupt("compressed page envelope truncated"));
    }
    let version = slot[6];
    if version != ENVELOPE_VERSION {
        return Err(corrupt(format!("unknown envelope version {version}")));
    }
    let codec = slot[7];
    if codec != CODEC_ZSTD && codec != CODEC_ZSTD_DICT {
        return Err(corrupt(format!("unknown envelope codec {codec}")));
    }
    let dict_id = u32::from_le_bytes(slot[8..12].try_into().expect("4 bytes"));
    let payload_len = u16::from_le_bytes(slot[12..14].try_into().expect("2 bytes")) as usize;
    let stored_crc = u32::from_le_bytes(slot[14..18].try_into().expect("4 bytes"));
    let end = ENVELOPE_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| corrupt("envelope payload length overflow"))?;
    if slot.len() < end {
        return Err(corrupt("envelope payload extends past the page slot"));
    }
    let payload = &slot[ENVELOPE_HEADER_LEN..end];
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
    fn zstd_bulk_round_trips_8k_image() {
        // Sanity-check the external zstd API shapes we rely on.
        let image: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let compressed = zstd::bulk::compress(&image, LEVEL_AT_REST).unwrap();
        assert!(compressed.len() < image.len());
        let restored = zstd::bulk::decompress(&compressed, image.len()).unwrap();
        assert_eq!(restored, image);
    }
}
