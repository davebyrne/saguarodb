use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
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
