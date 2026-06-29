//! `INTERVAL` support. PostgreSQL stores an interval as three independent
//! components — months, days, and microseconds — because a month is not a fixed
//! number of days and a day is not always 24 hours (the distinction matters when
//! adding an interval to a date/timestamp).
//!
//! For comparison, ordering, and hashing PostgreSQL collapses an interval to a
//! single canonical estimate (a month = 30 days, a day = 24 hours), so e.g.
//! `INTERVAL '1 mon'` and `INTERVAL '30 days'` compare *equal*. [`Interval`]
//! provides that total order with a consistent `Hash`, so `Value::Interval` keeps
//! `Value`'s derived `Ord`/`Eq`/`Hash` valid for keys, `DISTINCT`, and grouping
//! while still storing the exact components for display and arithmetic.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_DAY: i128 = 86_400 * MICROS_PER_SEC as i128;
/// PostgreSQL's canonical month length for interval comparison.
const DAYS_PER_MONTH_ESTIMATE: i128 = 30;

/// An `INTERVAL`: months, days, and microseconds kept separate (PostgreSQL's
/// model). Compared/hashed by the canonical estimate (see module docs).
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Interval {
            months,
            days,
            micros,
        }
    }

    pub const ZERO: Interval = Interval {
        months: 0,
        days: 0,
        micros: 0,
    };

    /// The canonical comparison value in microseconds (month = 30 days, day = 24
    /// hours), as `i128` so the products cannot overflow.
    fn estimate(&self) -> i128 {
        i128::from(self.months) * DAYS_PER_MONTH_ESTIMATE * MICROS_PER_DAY
            + i128::from(self.days) * MICROS_PER_DAY
            + i128::from(self.micros)
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.estimate().cmp(&other.estimate())
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.estimate() == other.estimate()
    }
}

impl Eq for Interval {}

impl Hash for Interval {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Must agree with `eq`: equal estimates hash alike (so `1 mon` and
        // `30 days` collide, as they compare equal).
        self.estimate().hash(state);
    }
}

/// Encode an interval in PostgreSQL's binary wire format: `int64` microseconds,
/// then `int32` days, then `int32` months (16 bytes, big-endian).
pub fn to_pg_binary(iv: &Interval) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&iv.micros.to_be_bytes());
    out[8..12].copy_from_slice(&iv.days.to_be_bytes());
    out[12..16].copy_from_slice(&iv.months.to_be_bytes());
    out
}

/// Decode PostgreSQL's binary interval format (see [`to_pg_binary`]).
pub fn from_pg_binary(bytes: &[u8]) -> Option<Interval> {
    if bytes.len() != 16 {
        return None;
    }
    Some(Interval {
        micros: i64::from_be_bytes(bytes[0..8].try_into().ok()?),
        days: i32::from_be_bytes(bytes[8..12].try_into().ok()?),
        months: i32::from_be_bytes(bytes[12..16].try_into().ok()?),
    })
}

/// Format an interval in PostgreSQL's default (`postgres`) style, e.g.
/// `1 year 2 mons 3 days 04:05:06`. A zero interval renders as `00:00:00`.
pub fn format_interval(iv: &Interval) -> String {
    let mut parts: Vec<String> = Vec::new();
    let years = iv.months / 12;
    let mons = iv.months % 12;
    if years != 0 {
        parts.push(format!("{years} year{}", plural(years)));
    }
    if mons != 0 {
        parts.push(format!("{mons} mon{}", plural(mons)));
    }
    if iv.days != 0 {
        parts.push(format!("{} day{}", iv.days, plural(iv.days)));
    }
    if iv.micros != 0 || parts.is_empty() {
        parts.push(format_interval_time(iv.micros));
    }
    parts.join(" ")
}

fn plural(n: i32) -> &'static str {
    if n.abs() == 1 { "" } else { "s" }
}

/// Format the microsecond component as `[-]HH:MM:SS[.ffffff]`; hours are not
/// wrapped to a day (an interval of 100 hours renders `100:00:00`).
fn format_interval_time(micros: i64) -> String {
    let negative = micros < 0;
    let abs = micros.unsigned_abs();
    let total_secs = abs / MICROS_PER_SEC as u64;
    let fraction = abs % MICROS_PER_SEC as u64;
    let (hours, minutes, secs) = (
        total_secs / 3_600,
        (total_secs % 3_600) / 60,
        total_secs % 60,
    );
    let mut out = format!(
        "{}{hours:02}:{minutes:02}:{secs:02}",
        if negative { "-" } else { "" }
    );
    if fraction != 0 {
        out.push('.');
        out.push_str(format!("{fraction:06}").trim_end_matches('0'));
    }
    out
}

/// Parse a PostgreSQL "verbose"/`postgres`-style interval literal, e.g.
/// `1 year 2 months 3 days`, `04:05:06`, `1 day 02:30:00`, `-1 day`, or
/// `1 day ago`. Supported units (singular/plural/common abbreviations):
/// year, month/mon, week, day, hour/hr, minute/min, second/sec; plus a
/// `HH:MM:SS[.ffffff]` time component. Quantities are integers (fractional unit
/// quantities like `1.5 hours` are not supported; use the time form). ISO-8601
/// (`P1Y2M…`) is not supported. Returns `None` for malformed input or overflow.
pub fn parse_interval(text: &str) -> Option<Interval> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;
    let mut negate_all = false;
    let mut saw_field = false;

    let mut tokens = text.split_whitespace();
    while let Some(token) = tokens.next() {
        if token.eq_ignore_ascii_case("ago") {
            negate_all = true;
            // `ago` must be the final token.
            if tokens.next().is_some() {
                return None;
            }
            break;
        }
        if token.contains(':') {
            micros = micros.checked_add(parse_interval_time(token)?)?;
            saw_field = true;
            continue;
        }
        // Otherwise a signed integer quantity followed by a unit token.
        let quantity: i64 = token.parse().ok()?;
        let unit = tokens.next()?;
        apply_unit(quantity, unit, &mut months, &mut days, &mut micros)?;
        saw_field = true;
    }
    if !saw_field {
        return None;
    }

    if negate_all {
        months = months.checked_neg()?;
        days = days.checked_neg()?;
        micros = micros.checked_neg()?;
    }
    Some(Interval {
        months: i32::try_from(months).ok()?,
        days: i32::try_from(days).ok()?,
        micros,
    })
}

/// Apply one `<quantity> <unit>` pair to the running components.
fn apply_unit(
    n: i64,
    unit: &str,
    months: &mut i64,
    days: &mut i64,
    micros: &mut i64,
) -> Option<()> {
    let unit = unit.to_ascii_lowercase();
    let unit = unit.strip_suffix('s').unwrap_or(&unit); // accept plurals
    match unit {
        "year" | "yr" => *months = months.checked_add(n.checked_mul(12)?)?,
        "month" | "mon" => *months = months.checked_add(n)?,
        "week" => *days = days.checked_add(n.checked_mul(7)?)?,
        "day" => *days = days.checked_add(n)?,
        "hour" | "hr" => *micros = micros.checked_add(n.checked_mul(3_600 * MICROS_PER_SEC)?)?,
        "minute" | "min" => *micros = micros.checked_add(n.checked_mul(60 * MICROS_PER_SEC)?)?,
        "second" | "sec" => *micros = micros.checked_add(n.checked_mul(MICROS_PER_SEC)?)?,
        _ => return None,
    }
    Some(())
}

/// Parse a `[-]HH:MM:SS[.ffffff]` interval time component into microseconds.
/// Hours are unbounded (not wrapped to a day) and may be negative.
fn parse_interval_time(token: &str) -> Option<i64> {
    let (negative, rest) = match token.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, token),
    };
    let mut parts = rest.splitn(3, ':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next()?.parse().ok()?;
    let seconds_field = parts.next()?;
    let (seconds_str, fraction) = match seconds_field.split_once('.') {
        Some((s, f)) => (s, Some(f)),
        None => (seconds_field, None),
    };
    let seconds: i64 = seconds_str.parse().ok()?;
    if hours < 0 || !(0..60).contains(&minutes) || !(0..60).contains(&seconds) {
        return None;
    }
    let fraction_micros = match fraction {
        None => 0,
        Some(f) => parse_fraction_micros(f)?,
    };
    let total = (hours.checked_mul(3_600)? + minutes * 60 + seconds)
        .checked_mul(MICROS_PER_SEC)?
        .checked_add(fraction_micros)?;
    Some(if negative { -total } else { total })
}

/// Digits after the decimal point as microseconds (`5` -> 500000); extra digits
/// beyond microsecond resolution are ignored.
fn parse_fraction_micros(fraction: &str) -> Option<i64> {
    if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let take = fraction.len().min(6);
    let value: i64 = fraction[..take].parse().ok()?;
    Some(value * 10_i64.pow(6 - take as u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn iv(months: i32, days: i32, micros: i64) -> Interval {
        Interval::new(months, days, micros)
    }
    fn hash_of(v: &Interval) -> u64 {
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    }

    #[test]
    fn one_month_equals_thirty_days_by_estimate() {
        let a = iv(1, 0, 0);
        let b = iv(0, 30, 0);
        assert_eq!(a, b); // equal by canonical estimate
        assert_eq!(hash_of(&a), hash_of(&b));
        // ...but ordering is by the estimate, so 31 days > 1 month.
        assert!(iv(0, 31, 0) > a);
        assert!(iv(0, 29, 0) < a);
    }

    #[test]
    fn parse_verbose_forms() {
        assert_eq!(parse_interval("1 year 2 months"), Some(iv(14, 0, 0)));
        assert_eq!(parse_interval("3 days"), Some(iv(0, 3, 0)));
        assert_eq!(parse_interval("1 week"), Some(iv(0, 7, 0)));
        assert_eq!(
            parse_interval("1 day 02:03:04"),
            Some(iv(0, 1, (2 * 3_600 + 3 * 60 + 4) * MICROS_PER_SEC))
        );
        assert_eq!(
            parse_interval("04:05:06"),
            Some(iv(0, 0, (4 * 3_600 + 5 * 60 + 6) * MICROS_PER_SEC))
        );
        assert_eq!(parse_interval("-1 day"), Some(iv(0, -1, 0)));
        assert_eq!(parse_interval("1 day ago"), Some(iv(0, -1, 0)));
        assert_eq!(
            parse_interval("90 minutes"),
            Some(iv(0, 0, 90 * 60 * MICROS_PER_SEC))
        );
        // Hours beyond a day are kept (not wrapped).
        assert_eq!(
            parse_interval("100:00:00"),
            Some(iv(0, 0, 100 * 3_600 * MICROS_PER_SEC))
        );
        assert_eq!(
            parse_interval("2 hours 30 mins"),
            Some(iv(0, 0, (2 * 3_600 + 30 * 60) * MICROS_PER_SEC))
        );
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_interval(""), None);
        assert_eq!(parse_interval("hello"), None);
        assert_eq!(parse_interval("1 fortnight"), None);
        assert_eq!(parse_interval("1"), None); // number with no unit
        assert_eq!(parse_interval("12:60:00"), None); // bad minutes
        assert_eq!(parse_interval("1 day ago extra"), None);
    }

    #[test]
    fn format_matches_postgres_style() {
        assert_eq!(
            format_interval(&iv(14, 3, (4 * 3_600 + 5 * 60 + 6) * MICROS_PER_SEC)),
            "1 year 2 mons 3 days 04:05:06"
        );
        assert_eq!(format_interval(&iv(0, 0, 0)), "00:00:00");
        assert_eq!(format_interval(&iv(1, 0, 0)), "1 mon");
        assert_eq!(format_interval(&iv(24, 0, 0)), "2 years");
        assert_eq!(format_interval(&iv(0, 1, 0)), "1 day");
        assert_eq!(format_interval(&iv(0, 0, -MICROS_PER_SEC)), "-00:00:01");
        assert_eq!(format_interval(&iv(0, 0, 500_000)), "00:00:00.5");
    }

    #[test]
    fn pg_binary_round_trips() {
        for v in [
            iv(0, 0, 0),
            iv(14, 3, 12_345_678),
            iv(-1, -2, -3_000_000),
            iv(0, 0, i64::MAX),
        ] {
            assert_eq!(from_pg_binary(&to_pg_binary(&v)), Some(v));
        }
        assert_eq!(from_pg_binary(&[0u8; 8]), None); // wrong length
    }

    #[test]
    fn parse_format_round_trip() {
        for text in [
            "1 year 2 mons 3 days 04:05:06",
            "1 mon",
            "2 years",
            "-00:00:01",
            "5 days",
        ] {
            let parsed = parse_interval(text).unwrap();
            assert_eq!(format_interval(&parsed), text);
        }
    }
}
