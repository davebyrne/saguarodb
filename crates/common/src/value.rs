use serde::{Deserialize, Serialize};

use crate::SqlArray;
use crate::float::{OrderedF32, OrderedF64};
use crate::interval::Interval;
use crate::numeric::Decimal;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    /// `DOUBLE PRECISION`, an IEEE 754 `f64` wrapped for a total order (NaN sorts
    /// greatest and equals itself, `-0.0 == +0.0`) so `Value`'s derived
    /// `Ord`/`Eq`/`Hash` stay valid for keys, `DISTINCT`, and grouping.
    Float(OrderedF64),
    /// `REAL` (single precision), an IEEE 754 `f32` wrapped for a total order
    /// (same NaN / signed-zero rules as `Float`).
    Real(OrderedF32),
    /// `NUMERIC` / `DECIMAL`, an exact base-10 value carrying its own scale.
    /// `Decimal` compares and hashes by value (`1.0` == `1.00`), so the derived
    /// `Ord`/`Eq`/`Hash` stay valid for keys, `DISTINCT`, and grouping.
    Numeric(Decimal),
    Text(String),
    /// `DATE`, stored as days from the Unix epoch (1970-01-01 = 0). i64-backed so
    /// the derived `Ord`/`Hash` give correct date ordering and key/dedup behavior.
    Date(i64),
    /// `TIMESTAMP` (without time zone), stored as microseconds from the Unix epoch
    /// (1970-01-01 00:00:00 = 0). i64-backed, like `Date`.
    Timestamp(i64),
    /// `TIME` (without time zone), stored as microseconds since midnight
    /// (`0..86_400_000_000`). i64-backed.
    Time(i64),
    /// `TIMESTAMP WITH TIME ZONE`, stored as microseconds from the Unix epoch in
    /// UTC (input offsets are normalized to UTC; always displayed in UTC).
    TimestampTz(i64),
    /// `INTERVAL` — months/days/microseconds kept separate; compares by the
    /// canonical estimate (so `1 mon` == `30 days`).
    Interval(Interval),
    /// `BYTEA` — a raw byte string. `Vec<u8>` ordering/hashing are lexicographic.
    Bytes(#[serde(deserialize_with = "crate::durable::deserialize_bounded_bytes")] Vec<u8>),
    /// `UUID`, stored as its 16 bytes. `[u8; 16]` ordering is the canonical
    /// (network-order) byte ordering, matching PostgreSQL.
    Uuid([u8; 16]),
    /// A homogeneous rectangular SQL array. Appended after every pre-array value
    /// variant because declaration order is the durable decoded B-tree ordering.
    Array(SqlArray),
}

/// Parse PostgreSQL boolean input text, returning `None` for unrecognized input
/// so each caller can map the failure to its own SQLSTATE (the protocol
/// extended-query path uses a protocol error; the `COPY` import path uses
/// `SqlState::InvalidTextRepresentation`). Surrounding whitespace is ignored and
/// matching is case-insensitive, matching PostgreSQL's `boolin`.
pub fn parse_bool_text(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn value_order_is_deterministic_across_variants() {
        let values = vec![
            Value::Text("a".to_string()),
            Value::Integer(7),
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
        ];

        let mut sorted = values.clone();
        sorted.sort();

        assert_eq!(
            sorted,
            vec![
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(true),
                Value::Integer(7),
                Value::Text("a".to_string()),
            ]
        );
    }
}
