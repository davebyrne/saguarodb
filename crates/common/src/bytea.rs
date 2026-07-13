//! Text I/O for the `BYTEA` type. SaguaroDB uses PostgreSQL's hex format only
//! (`\x` followed by an even number of hex digits); the legacy escape (`\nnn`)
//! format is not supported. Hex has been PostgreSQL's default `bytea_output`
//! and a valid input form since 9.0.

/// Format bytes as a hex bytea string: `\x` followed by lowercase hex digits
/// (two per byte). The empty byte string formats as `\x`.
pub fn format_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Parse hex bytea input: a `\x` prefix followed by an even number of hex digits.
/// Surrounding whitespace is ignored. Returns `None` for any other form (notably
/// the legacy escape format), an odd digit count, or a non-hex character.
pub fn parse_hex(text: &str) -> Option<Vec<u8>> {
    let hex = text.trim().strip_prefix("\\x")?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let digits = hex.as_bytes();
    let mut out = Vec::with_capacity(digits.len() / 2);
    let mut i = 0;
    while i < digits.len() {
        let hi = (digits[i] as char).to_digit(16)?;
        let lo = (digits[i + 1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
        i += 2;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_uses_lowercase_hex_with_prefix() {
        assert_eq!(format_hex(&[]), "\\x");
        assert_eq!(format_hex(&[0xde, 0xad, 0xbe, 0xef]), "\\xdeadbeef");
        assert_eq!(format_hex(&[0x00, 0x0f, 0xa0]), "\\x000fa0");
    }

    #[test]
    fn parse_accepts_hex_and_rejects_other_forms() {
        assert_eq!(parse_hex("\\x"), Some(vec![]));
        assert_eq!(parse_hex("\\xDEADbeef"), Some(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(parse_hex("  \\x00ff "), Some(vec![0x00, 0xff]));
        assert_eq!(parse_hex("\\xabc"), None); // odd digit count
        assert_eq!(parse_hex("\\xzz"), None); // non-hex
        assert_eq!(parse_hex("deadbeef"), None); // missing prefix
        assert_eq!(parse_hex("\\001"), None); // legacy escape not supported
    }

    #[test]
    fn round_trips() {
        for bytes in [vec![], vec![0x00], vec![0xff, 0x10, 0x00, 0x7f]] {
            assert_eq!(parse_hex(&format_hex(&bytes)), Some(bytes));
        }
    }
}
