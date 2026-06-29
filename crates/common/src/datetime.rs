//! Calendar math for the `DATE` type, stored as a count of days from the Unix
//! epoch (1970-01-01 = 0). Uses Howard Hinnant's `days_from_civil` /
//! `civil_from_days` algorithms over the proleptic Gregorian calendar, so no
//! external date dependency is needed. `TIMESTAMP` builds on this in a later
//! change.

/// Days from the Unix epoch (1970-01-01) for a proleptic-Gregorian Y-M-D.
/// `month` is 1..=12 and `day` is 1..=31; out-of-range components produce a
/// value that will not round-trip (callers validate via [`civil_from_days`]).
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let m = month as i64;
    let d = day as i64;
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Inverse of [`days_from_civil`]: the (year, month, day) for a day count from
/// the Unix epoch. `month` is 1..=12, `day` is 1..=31.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}

/// Parse a `YYYY-MM-DD` date into days-from-epoch, returning `None` for any
/// malformed or non-existent date (e.g. `2023-02-29`). Surrounding whitespace is
/// ignored. Only the ISO calendar-date form is accepted (no BC/era suffixes).
pub fn parse_date(text: &str) -> Option<i64> {
    let text = text.trim();
    let mut parts = text.splitn(3, '-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    // Round-trip to reject impossible days (month length, leap years).
    if civil_from_days(days) == (year, month, day) {
        Some(days)
    } else {
        None
    }
}

/// Format days-from-epoch as `YYYY-MM-DD` (zero-padded to at least 4 year digits).
pub fn format_date(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

/// Parse a `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` timestamp into microseconds from the
/// Unix epoch. The date and time may be separated by a space or `T`; the time is
/// optional (defaults to midnight) and the fractional seconds are optional (up to
/// microsecond resolution, extra digits ignored). Returns `None` for any
/// malformed or out-of-range input. No time zone is accepted.
pub fn parse_timestamp(text: &str) -> Option<i64> {
    let text = text.trim();
    let (date_part, time_part) = match text.find([' ', 'T']) {
        Some(idx) => (&text[..idx], Some(text[idx + 1..].trim())),
        None => (text, None),
    };
    let days = parse_date(date_part)?;
    let time_micros = match time_part {
        None => 0,
        Some(time) => parse_time_of_day(time)?,
    };
    days.checked_mul(MICROS_PER_DAY)?.checked_add(time_micros)
}

/// Parse `HH:MM:SS[.ffffff]` into microseconds since midnight (`0..MICROS_PER_DAY`).
fn parse_time_of_day(text: &str) -> Option<i64> {
    let mut parts = text.splitn(3, ':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next()?.parse().ok()?;
    let seconds_field = parts.next()?;
    let (seconds_str, fraction) = match seconds_field.split_once('.') {
        Some((seconds, fraction)) => (seconds, Some(fraction)),
        None => (seconds_field, None),
    };
    let seconds: i64 = seconds_str.parse().ok()?;
    if !(0..24).contains(&hours) || !(0..60).contains(&minutes) || !(0..60).contains(&seconds) {
        return None;
    }
    let fraction_micros = match fraction {
        None => 0,
        Some(fraction) => parse_fraction_micros(fraction)?,
    };
    Some((hours * 3_600 + minutes * 60 + seconds) * MICROS_PER_SEC + fraction_micros)
}

/// Parse the digits after the decimal point into microseconds (e.g. `5` ->
/// 500000, `123456` -> 123456). Digits beyond microsecond resolution are ignored.
fn parse_fraction_micros(fraction: &str) -> Option<i64> {
    if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let take = fraction.len().min(6);
    let value: i64 = fraction[..take].parse().ok()?;
    Some(value * 10_i64.pow(6 - take as u32))
}

/// Parse a `TIME` literal `HH:MM:SS[.ffffff]` into microseconds since midnight
/// (`0..MICROS_PER_DAY`). Surrounding whitespace is ignored.
pub fn parse_time(text: &str) -> Option<i64> {
    parse_time_of_day(text.trim())
}

/// Format microseconds-since-midnight as `HH:MM:SS[.ffffff]` (the fractional part
/// is shown only when non-zero, with trailing zeros trimmed).
pub fn format_time(micros_of_day: i64) -> String {
    let rest = micros_of_day.rem_euclid(MICROS_PER_DAY);
    let seconds = rest / MICROS_PER_SEC;
    let fraction = rest % MICROS_PER_SEC;
    let (hours, minutes, secs) = (seconds / 3_600, (seconds % 3_600) / 60, seconds % 60);
    let mut out = format!("{hours:02}:{minutes:02}:{secs:02}");
    if fraction != 0 {
        out.push('.');
        out.push_str(format!("{fraction:06}").trim_end_matches('0'));
    }
    out
}

/// Format microseconds-from-epoch as `YYYY-MM-DD HH:MM:SS[.ffffff]` (the
/// fractional part is shown only when non-zero, with trailing zeros trimmed).
pub fn format_timestamp(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let rest = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02} {}", format_time(rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_parse_format_round_trip() {
        let micros = parse_time("13:45:30.5").unwrap();
        assert_eq!(
            micros,
            (13 * 3_600 + 45 * 60 + 30) * MICROS_PER_SEC + 500_000
        );
        assert_eq!(format_time(micros), "13:45:30.5");
        assert_eq!(parse_time("  00:00:00 "), Some(0));
        assert_eq!(format_time(0), "00:00:00");
        assert_eq!(
            format_time(parse_time("23:59:59.999999").unwrap()),
            "23:59:59.999999"
        );
        assert_eq!(parse_time("25:00:00"), None); // hour out of range
        assert_eq!(parse_time("12:60:00"), None); // minute out of range
        assert_eq!(parse_time("noon"), None);
    }

    #[test]
    fn epoch_and_neighbors() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }

    #[test]
    fn known_dates_round_trip() {
        assert_eq!(days_from_civil(2000, 1, 1), 10957);
        for (y, m, d) in [(1, 1, 1), (1969, 7, 20), (2024, 2, 29), (2099, 12, 31)] {
            let days = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(days), (y, m, d), "round trip {y}-{m}-{d}");
        }
    }

    #[test]
    fn parse_accepts_valid_and_rejects_invalid() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(
            parse_date("  2024-02-29 "),
            Some(days_from_civil(2024, 2, 29))
        );
        assert_eq!(parse_date("2023-02-29"), None); // not a leap year
        assert_eq!(parse_date("2024-13-01"), None);
        assert_eq!(parse_date("2024-00-01"), None);
        assert_eq!(parse_date("2024-01-32"), None);
        assert_eq!(parse_date("2024-01-00"), None);
        assert_eq!(parse_date("2024-01"), None);
        assert_eq!(parse_date("2024-01-15-extra"), None);
        assert_eq!(parse_date("not-a-date"), None);
    }

    #[test]
    fn format_matches_parse() {
        assert_eq!(format_date(0), "1970-01-01");
        for s in ["1970-01-01", "2024-02-29", "1999-12-31", "0001-01-01"] {
            assert_eq!(format_date(parse_date(s).unwrap()), s);
        }
    }

    #[test]
    fn timestamp_epoch_and_components() {
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse_timestamp("1970-01-01"), Some(0)); // date-only -> midnight
        assert_eq!(parse_timestamp("1970-01-01T00:00:01"), Some(1_000_000)); // 'T' separator
        assert_eq!(
            parse_timestamp("2024-01-15 12:30:45"),
            Some(
                days_from_civil(2024, 1, 15) * 86_400_000_000
                    + (12 * 3600 + 30 * 60 + 45) * 1_000_000
            )
        );
    }

    #[test]
    fn timestamp_fractions_and_rejections() {
        assert_eq!(parse_timestamp("1970-01-01 00:00:00.5"), Some(500_000));
        assert_eq!(parse_timestamp("1970-01-01 00:00:00.123456"), Some(123_456));
        // Digits beyond microsecond resolution are ignored.
        assert_eq!(
            parse_timestamp("1970-01-01 00:00:00.1234569"),
            Some(123_456)
        );
        assert_eq!(parse_timestamp("2024-01-15 24:00:00"), None); // hour out of range
        assert_eq!(parse_timestamp("2024-01-15 12:60:00"), None); // minute out of range
        assert_eq!(parse_timestamp("2024-01-15 12:00:60"), None); // second out of range
        assert_eq!(parse_timestamp("2023-02-29 00:00:00"), None); // bad date part
        assert_eq!(parse_timestamp("2024-01-15 12:00"), None); // missing seconds
    }

    #[test]
    fn timestamp_format_round_trips() {
        for s in [
            "1970-01-01 00:00:00",
            "2024-01-15 12:30:45",
            "2024-02-29 23:59:59.123456",
            "1969-12-31 23:59:59", // pre-epoch (negative micros)
            "2000-01-01 00:00:00.5",
        ] {
            assert_eq!(format_timestamp(parse_timestamp(s).unwrap()), s);
        }
    }
}
