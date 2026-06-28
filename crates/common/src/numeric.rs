//! `NUMERIC` / `DECIMAL` support, backed by [`rust_decimal::Decimal`] (an exact
//! base-10 value with up to ~28–29 significant digits and a scale of 0–28).
//!
//! `Decimal` compares and hashes *by value* — `1.0`, `1.00`, and `1` are all
//! equal and hash alike — while still carrying its own display scale, so
//! [`Value::Numeric`](crate::Value) can live in `Value` with the derived
//! `Ord`/`Eq`/`Hash` and stay valid for B-tree keys, `DISTINCT`, and grouping.
//! `parse_numeric`/`format_numeric` are the text I/O helpers (format preserves
//! the value's scale, e.g. `1.50` stays `"1.50"`).

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
pub use rust_decimal::{Decimal, RoundingStrategy};

/// Exact `i64` -> `Decimal`.
pub fn from_i64(value: i64) -> Decimal {
    Decimal::from(value)
}

/// `Decimal` -> `f64` (lossy), or `None` if it cannot be represented.
pub fn to_f64(value: &Decimal) -> Option<f64> {
    value.to_f64()
}

/// `f64` -> `Decimal`, or `None` for `NaN`/infinity or an out-of-range magnitude.
pub fn from_f64(value: f64) -> Option<Decimal> {
    Decimal::from_f64(value)
}

/// `Decimal` -> `i64`, rounding ties away from zero (PostgreSQL's `numeric`
/// rounding), or `None` if the rounded value is out of `i64` range.
pub fn to_i64_rounded(value: &Decimal) -> Option<i64> {
    value
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
}

/// Parse a decimal from text (plain or scientific notation). Surrounding
/// whitespace is ignored. Returns `None` for malformed input or a magnitude that
/// does not fit `Decimal`.
pub fn parse_numeric(text: &str) -> Option<Decimal> {
    let trimmed = text.trim();
    // `from_str` rejects scientific notation; `from_str_exact` / the scientific
    // parser accepts it. Try the exact parser first, then scientific.
    Decimal::from_str_exact(trimmed)
        .or_else(|_| Decimal::from_scientific(trimmed))
        .ok()
}

/// Format a decimal as text, preserving its scale (trailing zeros are kept, so a
/// value with scale 2 renders like `"1.50"`).
pub fn format_numeric(value: &Decimal) -> String {
    value.to_string()
}

/// Coerce a value to a `NUMERIC` type modifier. For unconstrained `NUMERIC`
/// (`precision` is `None`) the value is returned unchanged. For `NUMERIC(p, s)`
/// the value is rounded to `scale` fractional digits (round-half-away-from-zero,
/// matching PostgreSQL) and padded to exactly `scale`; `None` is returned when
/// the integer part no longer fits `precision - scale` digits (an overflow the
/// caller maps to `NumericValueOutOfRange`). `scale <= precision` is assumed
/// (enforced when the type is parsed).
pub fn apply_typmod(value: Decimal, precision: Option<u32>, scale: u32) -> Option<Decimal> {
    let Some(p) = precision else {
        return Some(value);
    };
    let mut rounded = value.round_dp_with_strategy(scale, RoundingStrategy::MidpointAwayFromZero);
    // The integer part must fit `p - scale` digits, i.e. |value| < 10^(p - scale).
    let limit = pow10(i32::try_from(p - scale).ok()?)?;
    if rounded.abs() >= limit {
        return None;
    }
    rounded.rescale(scale); // pad/trim display scale to exactly `scale`
    Some(rounded)
}

// PostgreSQL binary `numeric` wire format (base-10000 "NBASE" digit groups).
const NBASE: u128 = 10_000;
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;

/// Encode a decimal in PostgreSQL's binary `numeric` format:
/// `int16 ndigits, int16 weight, uint16 sign, uint16 dscale, int16[ndigits]`
/// where each digit is a base-10000 group, most significant first.
pub fn to_pg_binary(value: &Decimal) -> Vec<u8> {
    let scale = value.scale();
    let sign = if value.is_sign_negative() && !value.is_zero() {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };
    let mut mantissa = value.mantissa().unsigned_abs();

    // Pad the fractional decimal digits up to a multiple of 4 so the decimal point
    // falls on a base-10000 group boundary.
    let frac_groups = scale.div_ceil(4) as usize;
    for _ in 0..(frac_groups * 4 - scale as usize) {
        mantissa *= 10;
    }

    // Extract base-10000 groups, least significant first.
    let mut digits: Vec<u16> = Vec::new();
    while mantissa > 0 {
        digits.push((mantissa % NBASE) as u16);
        mantissa /= NBASE;
    }
    // weight (base-10000 exponent of the most significant group) before trimming.
    let mut weight = digits.len() as isize - frac_groups as isize - 1;
    digits.reverse(); // most significant first
    while digits.first() == Some(&0) {
        digits.remove(0);
        weight -= 1; // dropping a leading zero group lowers the exponent
    }
    while digits.last() == Some(&0) {
        digits.pop(); // trailing zero groups don't affect weight or dscale
    }
    if digits.is_empty() {
        weight = 0; // zero value
    }

    let mut out = Vec::with_capacity(8 + digits.len() * 2);
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&(weight as i16).to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(scale as u16).to_be_bytes());
    for digit in &digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    out
}

/// Decode PostgreSQL's binary `numeric` format. Returns `None` for `NaN`,
/// malformed input, or a magnitude that does not fit `Decimal`.
pub fn from_pg_binary(bytes: &[u8]) -> Option<Decimal> {
    if bytes.len() < 8 {
        return None;
    }
    let read_i16 = |b: &[u8]| i16::from_be_bytes([b[0], b[1]]);
    let ndigits = read_i16(&bytes[0..2]);
    let weight = read_i16(&bytes[2..4]) as i32;
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]) as u32;
    if sign == NUMERIC_NAN {
        return None; // Decimal has no NaN
    }
    if sign != NUMERIC_POS && sign != NUMERIC_NEG {
        return None;
    }
    let ndigits = usize::try_from(ndigits).ok()?;
    if bytes.len() != 8 + ndigits * 2 {
        return None;
    }

    let mut value = Decimal::ZERO;
    for i in 0..ndigits {
        let off = 8 + i * 2;
        let digit = read_i16(&bytes[off..off + 2]);
        if !(0..10_000).contains(&digit) {
            return None;
        }
        // group exponent in base-10000, i.e. 10^(4 * exp10)
        let exp10 = 4 * (weight - i as i32);
        let term = Decimal::from(digit as u32).checked_mul(pow10(exp10)?)?;
        value = value.checked_add(term)?;
    }
    value.rescale(dscale);
    if sign == NUMERIC_NEG {
        value.set_sign_negative(true);
    }
    Some(value)
}

/// `10^exp` as a `Decimal`, or `None` if it does not fit.
fn pow10(exp: i32) -> Option<Decimal> {
    if exp >= 0 {
        let p = u32::try_from(exp).ok()?;
        let m = 10_i128.checked_pow(p)?;
        Some(Decimal::from_i128_with_scale(m, 0))
    } else {
        let scale = u32::try_from(-exp).ok()?;
        if scale > 28 {
            return None;
        }
        Some(Decimal::from_i128_with_scale(1, scale))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::str::FromStr;

    fn hash_of(value: &Decimal) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn equal_values_with_different_scale_are_eq_and_hash_alike() {
        // The invariant Value's derived Eq/Hash depend on: 1, 1.0, and 1.00 are
        // the same value and must hash identically.
        let a = Decimal::from_str("1").unwrap();
        let b = Decimal::from_str("1.0").unwrap();
        let c = Decimal::from_str("1.00").unwrap();
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(hash_of(&a), hash_of(&b));
        assert_eq!(hash_of(&b), hash_of(&c));
    }

    #[test]
    fn ordering_is_by_value_not_representation() {
        let mut v = [
            Decimal::from_str("2.5").unwrap(),
            Decimal::from_str("-1").unwrap(),
            Decimal::from_str("1.50").unwrap(),
            Decimal::from_str("1.5").unwrap(),
            Decimal::from_str("10").unwrap(),
        ];
        v.sort();
        let formatted: Vec<String> = v.iter().map(|d| d.to_string()).collect();
        // 1.5 and 1.50 are equal by value; both sort between -1 and 2.5.
        assert_eq!(formatted[0], "-1");
        assert!(formatted[1] == "1.5" || formatted[1] == "1.50");
        assert!(formatted[2] == "1.5" || formatted[2] == "1.50");
        assert_eq!(formatted[3], "2.5");
        assert_eq!(formatted[4], "10");
    }

    #[test]
    fn format_preserves_scale() {
        assert_eq!(format_numeric(&Decimal::from_str("1.50").unwrap()), "1.50");
        assert_eq!(format_numeric(&Decimal::from_str("1").unwrap()), "1");
        assert_eq!(
            format_numeric(&Decimal::from_str("-0.001").unwrap()),
            "-0.001"
        );
    }

    #[test]
    fn parse_accepts_plain_and_scientific_and_rejects_garbage() {
        assert_eq!(parse_numeric("3.14"), Decimal::from_str("3.14").ok());
        assert_eq!(parse_numeric("  -42 "), Decimal::from_str("-42").ok());
        assert_eq!(parse_numeric("1.5e3"), Decimal::from_str("1500").ok());
        assert_eq!(parse_numeric("abc"), None);
        assert_eq!(parse_numeric(""), None);
    }

    #[test]
    fn pg_binary_round_trips_and_matches_known_encodings() {
        // Known encodings: text, ndigits, weight, sign, dscale, digits.
        type Case = (&'static str, i16, i16, u16, u16, &'static [u16]);
        let cases: &[Case] = &[
            ("0", 0, 0, NUMERIC_POS, 0, &[]),
            ("1", 1, 0, NUMERIC_POS, 0, &[1]),
            ("1.50", 2, 0, NUMERIC_POS, 2, &[1, 5000]),
            ("0.5", 1, -1, NUMERIC_POS, 1, &[5000]),
            ("12345.678", 3, 1, NUMERIC_POS, 3, &[1, 2345, 6780]),
            ("10000", 1, 1, NUMERIC_POS, 0, &[1]),
            ("-2.5", 2, 0, NUMERIC_NEG, 1, &[2, 5000]),
        ];
        for (text, ndigits, weight, sign, dscale, groups) in cases {
            let d = Decimal::from_str(text).unwrap();
            let enc = to_pg_binary(&d);
            assert_eq!(
                i16::from_be_bytes([enc[0], enc[1]]),
                *ndigits,
                "ndigits {text}"
            );
            assert_eq!(
                i16::from_be_bytes([enc[2], enc[3]]),
                *weight,
                "weight {text}"
            );
            assert_eq!(u16::from_be_bytes([enc[4], enc[5]]), *sign, "sign {text}");
            assert_eq!(
                u16::from_be_bytes([enc[6], enc[7]]),
                *dscale,
                "dscale {text}"
            );
            let got: Vec<u16> = enc[8..]
                .chunks(2)
                .map(|c| u16::from_be_bytes([c[0], c[1]]))
                .collect();
            assert_eq!(got, *groups, "digits {text}");
            // Round-trip preserves value and display scale.
            let back = from_pg_binary(&enc).unwrap();
            assert_eq!(back, d, "value {text}");
            assert_eq!(back.to_string(), d.to_string(), "scale {text}");
        }
    }

    #[test]
    fn pg_binary_round_trips_many_values() {
        // Deterministic spread of mantissas/scales (no RNG in tests).
        for mantissa in [0_i128, 1, -1, 7, 123, -4096, 999999, -1000000, 271828182] {
            for scale in [0_u32, 1, 2, 4, 5, 9] {
                let d = Decimal::from_i128_with_scale(mantissa, scale);
                let back = from_pg_binary(&to_pg_binary(&d)).unwrap();
                assert_eq!(back, d, "value m={mantissa} s={scale}");
                assert_eq!(
                    back.to_string(),
                    d.to_string(),
                    "scale m={mantissa} s={scale}"
                );
            }
        }
    }

    #[test]
    fn from_pg_binary_rejects_nan_and_malformed() {
        let nan = [0, 0, 0, 0, 0xC0, 0x00, 0, 0]; // sign = NUMERIC_NAN
        assert_eq!(from_pg_binary(&nan), None);
        assert_eq!(from_pg_binary(&[0, 1]), None); // too short
        // ndigits says 1 group but no group bytes follow.
        assert_eq!(from_pg_binary(&[0, 1, 0, 0, 0, 0, 0, 0]), None);
    }

    #[test]
    fn apply_typmod_rounds_pads_and_overflows() {
        let d = |s: &str| Decimal::from_str(s).unwrap();
        // Unconstrained: unchanged.
        assert_eq!(apply_typmod(d("1.239"), None, 0), Some(d("1.239")));
        // NUMERIC(10,2): rounds half-away-from-zero and pads to scale 2.
        assert_eq!(
            apply_typmod(d("1.239"), Some(10), 2).unwrap().to_string(),
            "1.24"
        );
        assert_eq!(
            apply_typmod(d("1.005"), Some(10), 2).unwrap().to_string(),
            "1.01"
        );
        assert_eq!(
            apply_typmod(d("1"), Some(10), 2).unwrap().to_string(),
            "1.00"
        );
        assert_eq!(
            apply_typmod(d("-2.5"), Some(4), 0).unwrap().to_string(),
            "-3"
        ); // ties away
        // Overflow: NUMERIC(4,2) allows |v| < 100.
        assert_eq!(
            apply_typmod(d("99.99"), Some(4), 2).unwrap().to_string(),
            "99.99"
        );
        assert_eq!(apply_typmod(d("100"), Some(4), 2), None);
        assert_eq!(apply_typmod(d("-100"), Some(4), 2), None);
        // NUMERIC(2,2): only |v| < 1.
        assert_eq!(
            apply_typmod(d("0.99"), Some(2), 2).unwrap().to_string(),
            "0.99"
        );
        assert_eq!(apply_typmod(d("1.00"), Some(2), 2), None);
    }

    #[test]
    fn round_half_away_from_zero_matches_postgres() {
        let two = |s: &str| {
            Decimal::from_str(s)
                .unwrap()
                .round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero)
        };
        assert_eq!(two("1.005").to_string(), "1.01");
        assert_eq!(two("2.675").to_string(), "2.68");
        assert_eq!(two("-1.005").to_string(), "-1.01");
    }
}
