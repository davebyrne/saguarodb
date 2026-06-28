//! `f64` support for `DOUBLE PRECISION`.
//!
//! `f64` does not implement `Ord`, `Eq`, or `Hash` (NaN is unordered and not
//! reflexively equal, and `-0.0`/`+0.0` are distinct bit patterns that compare
//! equal). [`Value`](crate::Value) derives all three and relies on them for
//! B-tree keys, `DISTINCT`, `GROUP BY`, and `ORDER BY`, so a bare `f64` cannot
//! live in it. [`OrderedF64`] wraps `f64` with a *total* order matching
//! PostgreSQL's float btree semantics — NaN sorts greatest and equals itself,
//! and `-0.0 == +0.0` — with a `Hash` consistent with that equality.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

/// An `f64` with a total order (see module docs). Construct with [`OrderedF64::new`]
/// or `OrderedF64(x)`; read the value with [`OrderedF64::get`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderedF64(pub f64);

impl OrderedF64 {
    #[inline]
    pub fn new(value: f64) -> Self {
        OrderedF64(value)
    }

    #[inline]
    pub fn get(self) -> f64 {
        self.0
    }
}

impl From<f64> for OrderedF64 {
    fn from(value: f64) -> Self {
        OrderedF64(value)
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater, // NaN sorts after every number
            (false, true) => Ordering::Less,
            // Neither is NaN, so `partial_cmp` is always `Some`; `-0.0`/`+0.0`
            // compare `Equal` here, matching the equality below.
            (false, false) => self.0.partial_cmp(&other.0).expect("non-NaN comparison"),
        }
    }
}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrderedF64 {}

impl Hash for OrderedF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Must agree with `eq`: all NaNs hash alike, and `-0.0`/`+0.0` hash alike.
        let bits = if self.0.is_nan() {
            f64::NAN.to_bits()
        } else if self.0 == 0.0 {
            0
        } else {
            self.0.to_bits()
        };
        bits.hash(state);
    }
}

/// Format a double for text output. Finite values use a round-trippable form:
/// fixed-point notation for moderate magnitudes (base-10 exponent in `[-4, 15]`,
/// matching PostgreSQL and avoiding the pathologically long decimal expansions
/// that plain `Display` produces for extreme exponents), and scientific notation
/// — spelled `e±NN` with at least two exponent digits — outside that range.
/// Non-finite values use PostgreSQL's `Infinity`/`-Infinity`/`NaN` spellings.
pub fn format_double(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value < 0.0 { "-Infinity" } else { "Infinity" }.to_string();
    }
    // Rust's `{:e}` gives the shortest significant digits with an exact base-10
    // exponent; the fixed form `{}` is the shortest fixed-point that round-trips.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` output always contains an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("`{:e}` exponent is a base-10 integer");
    if (-4..=15).contains(&exponent) {
        format!("{value}")
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

/// Parse a double from text. Accepts decimal and scientific notation plus the
/// `Infinity`/`-Infinity`/`NaN` spellings (case-insensitive, surrounding
/// whitespace ignored).
pub fn parse_double(text: &str) -> Option<f64> {
    text.trim().parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(value: f64) -> u64 {
        let mut hasher = DefaultHasher::new();
        OrderedF64(value).hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn total_order_places_nan_last_and_equal_to_itself() {
        let mut values = [
            OrderedF64(1.0),
            OrderedF64(f64::NAN),
            OrderedF64(-1.0),
            OrderedF64(f64::INFINITY),
            OrderedF64(f64::NEG_INFINITY),
        ];
        values.sort();
        let ordered: Vec<f64> = values.iter().map(|v| v.0).collect();
        assert_eq!(ordered[0], f64::NEG_INFINITY);
        assert_eq!(ordered[1], -1.0);
        assert_eq!(ordered[2], 1.0);
        assert_eq!(ordered[3], f64::INFINITY);
        assert!(ordered[4].is_nan()); // NaN sorts last
        assert_eq!(OrderedF64(f64::NAN), OrderedF64(f64::NAN)); // reflexive
    }

    #[test]
    fn negative_and_positive_zero_are_equal_and_hash_alike() {
        assert_eq!(OrderedF64(-0.0), OrderedF64(0.0));
        assert_eq!(OrderedF64(-0.0).cmp(&OrderedF64(0.0)), Ordering::Equal);
        assert_eq!(hash_of(-0.0), hash_of(0.0));
    }

    #[test]
    fn equal_values_hash_alike() {
        assert_eq!(hash_of(3.5), hash_of(3.5));
        assert_eq!(hash_of(f64::NAN), hash_of(f64::NAN));
    }

    #[test]
    fn format_handles_finite_and_non_finite() {
        assert_eq!(format_double(42.0), "42");
        assert_eq!(format_double(1.5), "1.5");
        assert_eq!(format_double(-0.25), "-0.25");
        assert_eq!(format_double(-0.0), "-0"); // sign of zero preserved
        assert_eq!(format_double(f64::INFINITY), "Infinity");
        assert_eq!(format_double(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(format_double(f64::NAN), "NaN");
    }

    #[test]
    fn format_uses_scientific_only_for_extreme_exponents() {
        // Moderate magnitudes stay fixed-point (no pathological expansions).
        assert_eq!(format_double(100000.0), "100000");
        assert_eq!(format_double(0.0001), "0.0001");
        assert_eq!(format_double(1e15), "1000000000000000");
        // Extreme exponents use scientific notation spelled `e±NN`.
        assert_eq!(format_double(1e16), "1e+16");
        assert_eq!(format_double(1e308), "1e+308");
        assert_eq!(format_double(1e-5), "1e-05");
        assert_eq!(format_double(-2.5e-300), "-2.5e-300");
        // ...and these still round-trip exactly.
        for value in [1e16, 1e308, 1e-5, 5e-324, -2.5e-300] {
            assert_eq!(parse_double(&format_double(value)), Some(value));
        }
    }

    #[test]
    fn parse_accepts_numbers_and_special_values() {
        assert_eq!(parse_double("2.75"), Some(2.75));
        assert_eq!(parse_double("  1e10 "), Some(1e10));
        assert_eq!(parse_double("-0.5"), Some(-0.5));
        assert_eq!(parse_double("Infinity"), Some(f64::INFINITY));
        assert_eq!(parse_double("-infinity"), Some(f64::NEG_INFINITY));
        assert!(parse_double("nan").unwrap().is_nan());
        assert_eq!(parse_double("abc"), None);
        assert_eq!(parse_double(""), None);
    }

    #[test]
    fn finite_values_round_trip_through_format() {
        for value in [0.0, 1.0, -1.0, 9.876543210987654, 1e-300, 1e300, 123456.789] {
            assert_eq!(parse_double(&format_double(value)), Some(value));
        }
    }
}
