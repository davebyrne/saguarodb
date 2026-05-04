use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
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
