//! Text I/O for the `UUID` type, stored as a 16-byte array. Output is the
//! canonical lowercase hyphenated form (`8-4-4-4-12`); input is lenient
//! (case-insensitive, optional surrounding braces, hyphens optional) matching
//! PostgreSQL's permissive `uuid_in`.

/// Format a 16-byte UUID as the canonical lowercase hyphenated string,
/// e.g. `0a0b0c0d-0e0f-1011-1213-141516171819`.
pub fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Parse a UUID string into its 16 bytes. Accepts the canonical hyphenated form
/// and a bare 32-hex-digit form, case-insensitive, with optional surrounding
/// whitespace and braces. Returns `None` if the result is not exactly 32 hex
/// digits.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(trimmed);

    let mut digits = [0u8; 32];
    let mut count = 0;
    for ch in inner.chars() {
        if ch == '-' {
            continue;
        }
        let nibble = ch.to_digit(16)?;
        if count == 32 {
            return None; // too many hex digits
        }
        digits[count] = nibble as u8;
        count += 1;
    }
    if count != 32 {
        return None;
    }

    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = digits[2 * i] * 16 + digits[2 * i + 1];
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_is_canonical_lowercase() {
        assert_eq!(
            format_uuid(&[0; 16]),
            "00000000-0000-0000-0000-000000000000"
        );
        let bytes = [
            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19,
        ];
        assert_eq!(format_uuid(&bytes), "0a0b0c0d-0e0f-1011-1213-141516171819");
    }

    #[test]
    fn parse_accepts_lenient_forms() {
        let canonical = "0a0b0c0d-0e0f-1011-1213-141516171819";
        let expected = [
            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19,
        ];
        assert_eq!(parse_uuid(canonical), Some(expected));
        assert_eq!(
            parse_uuid("0A0B0C0D-0E0F-1011-1213-141516171819"),
            Some(expected)
        ); // upper
        assert_eq!(
            parse_uuid("0a0b0c0d0e0f10111213141516171819"),
            Some(expected)
        ); // no hyphens
        assert_eq!(
            parse_uuid("  {0a0b0c0d-0e0f-1011-1213-141516171819}  "),
            Some(expected)
        ); // braces + whitespace
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert_eq!(parse_uuid("not-a-uuid"), None);
        assert_eq!(parse_uuid("0a0b"), None); // too short
        assert_eq!(parse_uuid("0a0b0c0d0e0f10111213141516171819ff"), None); // too long
        assert_eq!(parse_uuid("0a0b0c0d-0e0f-1011-1213-1415161718zz"), None); // non-hex
    }

    #[test]
    fn round_trips() {
        let s = "12345678-9abc-def0-1234-56789abcdef0";
        assert_eq!(format_uuid(&parse_uuid(s).unwrap()), s);
    }
}
