use std::collections::HashMap;
use std::sync::Arc;

use common::{DbError, FileId, Result, SqlState};
use parking_lot::RwLock;
use zstd::dict::{DecoderDictionary, EncoderDictionary};

use crate::codec::{
    CODEC_NONE, CODEC_ZSTD, CODEC_ZSTD_DICT, LEVEL_AT_REST, LEVEL_WAL, decode_envelope,
    decompress_value_zstd, encode_envelope, ensure_value_len, is_envelope,
};
#[cfg(test)]
use crate::dict::train_dictionary;

/// A table file's at-rest compression config (`compression.md` §4/§5a).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FileCompression {
    #[default]
    None,
    Zstd {
        dict_id: Option<u32>,
    },
}

/// Prepared per-level dictionary handles (dict processing is amortized once).
struct LoadedDictionary {
    enc_rest: EncoderDictionary<'static>,
    enc_wal: EncoderDictionary<'static>,
    dec: DecoderDictionary<'static>,
}

/// Shared FileId → config map plus dictionary resolver. One instance is
/// created by the server and injected into both `HeapPageStore` (at-rest) and
/// `PageBackedStorageEngine` (WAL FPIs). A file with no entry reads/writes raw.
#[derive(Default)]
pub struct CompressionRegistry {
    files: RwLock<HashMap<FileId, FileCompression>>,
    dicts: RwLock<HashMap<u32, Arc<LoadedDictionary>>>,
}

// `CompressionRegistry` is shared across the server's storage/WAL threads, so
// it must be `Send + Sync`. Lock this in at the declaration so a future
// non-Send/Sync field fails to compile here, not in a downstream crate.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CompressionRegistry>();
};

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

impl CompressionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_file_config(&self, file_id: FileId, config: FileCompression) {
        self.files.write().insert(file_id, config);
    }

    pub fn file_config(&self, file_id: FileId) -> FileCompression {
        self.files.read().get(&file_id).copied().unwrap_or_default()
    }

    pub fn register_dictionary(&self, dict_id: u32, bytes: &[u8]) -> Result<()> {
        if dict_id == 0 {
            return Err(corrupt("dictionary id 0 is reserved"));
        }
        let loaded = LoadedDictionary {
            enc_rest: EncoderDictionary::copy(bytes, LEVEL_AT_REST),
            enc_wal: EncoderDictionary::copy(bytes, LEVEL_WAL),
            dec: DecoderDictionary::copy(bytes),
        };
        self.dicts.write().insert(dict_id, Arc::new(loaded));
        Ok(())
    }

    pub fn has_dictionary(&self, dict_id: u32) -> bool {
        self.dicts.read().contains_key(&dict_id)
    }

    fn dictionary(&self, dict_id: u32) -> Option<Arc<LoadedDictionary>> {
        self.dicts.read().get(&dict_id).cloned()
    }

    /// Registered-and-resolvable dict for a file, if its config names one.
    fn dict_for_file(&self, file_id: FileId) -> Option<(u32, Arc<LoadedDictionary>)> {
        match self.file_config(file_id) {
            FileCompression::Zstd { dict_id: Some(id) } => self.dictionary(id).map(|d| (id, d)),
            _ => None,
        }
    }

    /// At-rest encode per the file's config. `Ok(None)` = store the raw image
    /// (config None, or the envelope is not smaller than the image).
    pub fn compress_page_at_rest(&self, file_id: FileId, image: &[u8]) -> Result<Option<Vec<u8>>> {
        let config = self.file_config(file_id);
        if config == FileCompression::None {
            return Ok(None);
        }
        let (codec, dict_id, payload) = match self.dict_for_file(file_id) {
            Some((id, dict)) => {
                let payload = zstd::bulk::Compressor::with_prepared_dictionary(&dict.enc_rest)
                    .and_then(|mut c| c.compress(image))
                    .map_err(|err| corrupt(format!("zstd compress failed: {err}")))?;
                (CODEC_ZSTD_DICT, id, payload)
            }
            None => {
                let payload = zstd::bulk::compress(image, LEVEL_AT_REST)
                    .map_err(|err| corrupt(format!("zstd compress failed: {err}")))?;
                (CODEC_ZSTD, 0, payload)
            }
        };
        if crate::codec::ENVELOPE_HEADER_LEN + payload.len() >= image.len() {
            return Ok(None);
        }
        encode_envelope(codec, dict_id, &payload).map(Some)
    }

    /// Decode a page slot read from disk. `Ok(None)` = raw slot (not an
    /// envelope, including all-zero holes). `Err` = corrupt envelope,
    /// unresolvable dictionary, or wrong decompressed length.
    pub fn decompress_page(&self, slot: &[u8], expected_len: usize) -> Result<Option<Vec<u8>>> {
        if !is_envelope(slot) {
            return Ok(None);
        }
        let envelope = decode_envelope(slot)?;
        let image = self.decompress_fpi(
            envelope.codec,
            envelope.dict_id,
            envelope.payload,
            expected_len,
        )?;
        Ok(Some(image))
    }

    /// UNCONDITIONAL WAL FPI compression (zstd-1; a heap file's dict when it
    /// has one). `None` = emit the raw `FullPageImage` (payload not smaller,
    /// or any internal failure — compression must never fail a write).
    pub fn compress_fpi(&self, file_id: FileId, image: &[u8]) -> Option<(u8, u32, Vec<u8>)> {
        let (codec, dict_id, payload) = match self.dict_for_file(file_id) {
            Some((id, dict)) => {
                let payload = zstd::bulk::Compressor::with_prepared_dictionary(&dict.enc_wal)
                    .and_then(|mut c| c.compress(image))
                    .ok()?;
                (CODEC_ZSTD_DICT, id, payload)
            }
            None => {
                let payload = zstd::bulk::compress(image, LEVEL_WAL).ok()?;
                (CODEC_ZSTD, 0, payload)
            }
        };
        if payload.len() >= image.len() {
            return None;
        }
        Some((codec, dict_id, payload))
    }

    /// Compress a single TOAST value payload with the registered dictionary.
    ///
    /// The caller owns all policy decisions: this helper always returns the
    /// compressed bytes if zstd succeeds, even when the output is larger than
    /// the raw input.
    pub fn compress_value_zstd_dict(&self, dict_id: u32, raw: &[u8]) -> Result<Vec<u8>> {
        let dict = self.dictionary(dict_id).ok_or_else(|| {
            corrupt(format!(
                "compression dictionary {dict_id} is not registered"
            ))
        })?;
        zstd::bulk::Compressor::with_prepared_dictionary(&dict.enc_rest)
            .and_then(|mut c| c.compress(raw))
            .map_err(|err| corrupt(format!("zstd dict value compress failed: {err}")))
    }

    /// Decompress a single TOAST value payload to exactly `raw_len` bytes.
    pub fn decompress_value(
        &self,
        codec: u8,
        dict_id: Option<u32>,
        payload: &[u8],
        raw_len: usize,
    ) -> Result<Vec<u8>> {
        let value = match codec {
            CODEC_NONE => {
                if let Some(dict_id) = dict_id {
                    return Err(corrupt(format!(
                        "raw value codec must not name dictionary {dict_id}"
                    )));
                }
                ensure_value_len(payload, raw_len)?;
                payload.to_vec()
            }
            CODEC_ZSTD => {
                if let Some(dict_id) = dict_id {
                    return Err(corrupt(format!(
                        "zstd value codec must not name dictionary {dict_id}"
                    )));
                }
                decompress_value_zstd(payload, raw_len)?
            }
            CODEC_ZSTD_DICT => {
                let dict_id =
                    dict_id.ok_or_else(|| corrupt("zstd-dict value codec missing dict id"))?;
                let dict = self.dictionary(dict_id).ok_or_else(|| {
                    corrupt(format!(
                        "compression dictionary {dict_id} is not registered"
                    ))
                })?;
                let value = zstd::bulk::Decompressor::with_prepared_dictionary(&dict.dec)
                    .and_then(|mut d| d.decompress(payload, raw_len))
                    .map_err(|err| corrupt(format!("zstd dict value decompress failed: {err}")))?;
                ensure_value_len(&value, raw_len)?;
                value
            }
            other => return Err(corrupt(format!("unknown value compression codec {other}"))),
        };
        Ok(value)
    }

    /// Decompress a compressed payload (WAL record or envelope body) back to
    /// exactly `expected_len` bytes.
    pub fn decompress_fpi(
        &self,
        codec: u8,
        dict_id: u32,
        payload: &[u8],
        expected_len: usize,
    ) -> Result<Vec<u8>> {
        let image = match codec {
            CODEC_ZSTD => zstd::bulk::decompress(payload, expected_len)
                .map_err(|err| corrupt(format!("zstd decompress failed: {err}")))?,
            CODEC_ZSTD_DICT => {
                let dict = self.dictionary(dict_id).ok_or_else(|| {
                    corrupt(format!(
                        "compression dictionary {dict_id} is not registered"
                    ))
                })?;
                zstd::bulk::Decompressor::with_prepared_dictionary(&dict.dec)
                    .and_then(|mut d| d.decompress(payload, expected_len))
                    .map_err(|err| corrupt(format!("zstd dict decompress failed: {err}")))?
            }
            other => return Err(corrupt(format!("unknown compression codec {other}"))),
        };
        if image.len() != expected_len {
            return Err(corrupt(format!(
                "decompressed page is {} bytes, expected {expected_len}",
                image.len()
            )));
        }
        Ok(image)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress_value_zstd;

    // Repetitive page-like images compress well even at level 1.
    fn sample_image(seed: u8) -> Vec<u8> {
        let row = format!("user-{seed}-payload-abcdefghijklmnopqrstuvwxyz;");
        row.as_bytes().iter().copied().cycle().take(8192).collect()
    }

    #[test]
    fn at_rest_respects_file_config_and_round_trips() {
        let registry = CompressionRegistry::new();
        let image = sample_image(1);

        // No config => raw.
        assert!(
            registry
                .compress_page_at_rest(10, &image)
                .unwrap()
                .is_none()
        );

        registry.set_file_config(10, FileCompression::Zstd { dict_id: None });
        let env = registry.compress_page_at_rest(10, &image).unwrap().unwrap();
        assert!(env.len() < image.len());
        let restored = registry
            .decompress_page(&env, image.len())
            .unwrap()
            .unwrap();
        assert_eq!(restored, image);

        // A raw slot decodes to None (not an envelope).
        assert!(
            registry
                .decompress_page(&image, image.len())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn incompressible_image_stays_raw() {
        let registry = CompressionRegistry::new();
        registry.set_file_config(10, FileCompression::Zstd { dict_id: None });
        // High-entropy bytes: xorshift-ish PRNG, no repetition.
        let mut x: u64 = 0x9E3779B97F4A7C15;
        let image: Vec<u8> = (0..8192)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x & 0xFF) as u8
            })
            .collect();
        assert!(
            registry
                .compress_page_at_rest(10, &image)
                .unwrap()
                .is_none()
        );
        assert!(registry.compress_fpi(10, &image).is_none());
    }

    #[test]
    fn fpi_compression_is_unconditional_and_uses_dict_when_registered() {
        let registry = CompressionRegistry::new();
        let image = sample_image(2);

        // No config at all: still compressed, dict-less zstd.
        let (codec, dict_id, payload) = registry.compress_fpi(99, &image).unwrap();
        assert_eq!(codec, CODEC_ZSTD);
        assert_eq!(dict_id, 0);
        assert_eq!(
            registry
                .decompress_fpi(codec, dict_id, &payload, image.len())
                .unwrap(),
            image
        );

        // With a trained dict registered and configured: codec 2 + dict id.
        let samples: Vec<Vec<u8>> = (0..64).map(|i| sample_image(i as u8)).collect();
        let dict = train_dictionary(&samples).expect("corpus is large enough");
        registry.register_dictionary(7, &dict).unwrap();
        registry.set_file_config(99, FileCompression::Zstd { dict_id: Some(7) });
        let (codec, dict_id, payload) = registry.compress_fpi(99, &image).unwrap();
        assert_eq!((codec, dict_id), (CODEC_ZSTD_DICT, 7));
        assert_eq!(
            registry
                .decompress_fpi(codec, dict_id, &payload, image.len())
                .unwrap(),
            image
        );

        // Unknown dict id on decompress is a structured error.
        assert!(
            registry
                .decompress_fpi(CODEC_ZSTD_DICT, 42, &payload, image.len())
                .is_err()
        );
    }

    #[test]
    fn dict_at_rest_round_trips() {
        let registry = CompressionRegistry::new();
        let samples: Vec<Vec<u8>> = (0..64).map(|i| sample_image(i as u8)).collect();
        let dict = train_dictionary(&samples).unwrap();
        registry.register_dictionary(3, &dict).unwrap();
        registry.set_file_config(5, FileCompression::Zstd { dict_id: Some(3) });

        let image = sample_image(9);
        let env = registry.compress_page_at_rest(5, &image).unwrap().unwrap();
        let restored = registry
            .decompress_page(&env, image.len())
            .unwrap()
            .unwrap();
        assert_eq!(restored, image);
    }

    fn sample_value(seed: u8) -> Vec<u8> {
        let chunk = format!("toast-value-{seed}-name-email-bio-location;");
        chunk.as_bytes().iter().copied().cycle().take(768).collect()
    }

    #[test]
    fn value_zstd_round_trips() {
        let registry = CompressionRegistry::new();
        let raw = sample_value(1);
        let payload = compress_value_zstd(&raw).unwrap();

        let restored = registry
            .decompress_value(CODEC_ZSTD, None, &payload, raw.len())
            .unwrap();
        assert_eq!(restored, raw);
    }

    #[test]
    fn value_raw_payload_round_trips() {
        let registry = CompressionRegistry::new();
        let raw = sample_value(2);

        let restored = registry
            .decompress_value(CODEC_NONE, None, &raw, raw.len())
            .unwrap();
        assert_eq!(restored, raw);
    }

    #[test]
    fn value_zstd_dict_round_trips() {
        let registry = CompressionRegistry::new();
        let samples: Vec<Vec<u8>> = (0..64).map(|i| sample_value(i as u8)).collect();
        let dict = train_dictionary(&samples).unwrap();
        registry.register_dictionary(11, &dict).unwrap();

        let raw = sample_value(3);
        let payload = registry.compress_value_zstd_dict(11, &raw).unwrap();
        let restored = registry
            .decompress_value(CODEC_ZSTD_DICT, Some(11), &payload, raw.len())
            .unwrap();
        assert_eq!(restored, raw);
    }

    #[test]
    fn value_decompress_rejects_wrong_raw_len() {
        let registry = CompressionRegistry::new();
        let raw = sample_value(4);
        let payload = compress_value_zstd(&raw).unwrap();

        let err = registry
            .decompress_value(CODEC_ZSTD, None, &payload, raw.len() + 1)
            .unwrap_err();
        assert_eq!(err.kind, common::ErrorKind::Storage);
        assert_eq!(err.code, common::SqlState::InternalError);

        assert!(
            registry
                .decompress_value(CODEC_NONE, None, &raw, raw.len() + 1)
                .is_err()
        );
    }

    #[test]
    fn value_decompress_rejects_unknown_codec() {
        let registry = CompressionRegistry::new();
        let err = registry
            .decompress_value(99, None, b"payload", 7)
            .unwrap_err();
        assert_eq!(err.kind, common::ErrorKind::Storage);
        assert_eq!(err.code, common::SqlState::InternalError);
    }

    #[test]
    fn value_decompress_validates_dictionary_metadata() {
        let registry = CompressionRegistry::new();
        let raw = sample_value(5);
        let payload = compress_value_zstd(&raw).unwrap();

        assert!(
            registry
                .decompress_value(CODEC_ZSTD, Some(1), &payload, raw.len())
                .is_err()
        );
        assert!(
            registry
                .decompress_value(CODEC_ZSTD_DICT, None, &payload, raw.len())
                .is_err()
        );
        assert!(registry.compress_value_zstd_dict(1, &raw).is_err());
    }

    #[test]
    fn train_dictionary_returns_none_on_tiny_corpus() {
        assert!(train_dictionary(&[]).is_none());
        assert!(train_dictionary(&[vec![1, 2, 3]]).is_none());
    }
}
