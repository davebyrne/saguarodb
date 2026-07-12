use std::cmp::Ordering;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{DataType, DbError, Result, SqlState, Value};

/// PostgreSQL-compatible maximum number of array dimensions.
pub const MAX_ARRAY_DIMENSIONS: usize = 6;
/// Practical allocation guard for one SQL array value.
pub const MAX_ARRAY_ELEMENTS: usize = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArrayDimension {
    len: u32,
    lower_bound: i32,
}

impl ArrayDimension {
    #[must_use]
    pub fn new(len: u32, lower_bound: i32) -> Self {
        Self { len, lower_bound }
    }

    #[must_use]
    pub fn len(self) -> u32 {
        self.len
    }

    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub fn lower_bound(self) -> i32 {
        self.lower_bound
    }
}

/// A rectangular SQL array. Elements are stored in row-major order; dimensions
/// carry PostgreSQL lower bounds independently of that flat storage.
#[derive(Clone, Debug, Serialize)]
pub struct SqlArray {
    element_type: DataType,
    dimensions: Vec<ArrayDimension>,
    elements: Vec<Value>,
}

impl SqlArray {
    pub fn new(
        element_type: DataType,
        dimensions: Vec<ArrayDimension>,
        elements: Vec<Value>,
    ) -> Result<Self> {
        if matches!(element_type, DataType::Array(_)) {
            return Err(invalid_array("array elements cannot themselves be arrays"));
        }
        if dimensions.len() > MAX_ARRAY_DIMENSIONS {
            return Err(invalid_array(format!(
                "array has {} dimensions, maximum is {MAX_ARRAY_DIMENSIONS}",
                dimensions.len()
            )));
        }
        if elements.len() > MAX_ARRAY_ELEMENTS {
            return Err(invalid_array(format!(
                "array has {} elements, maximum is {MAX_ARRAY_ELEMENTS}",
                elements.len()
            )));
        }
        if elements.is_empty() {
            if dimensions.iter().any(|dimension| !dimension.is_empty()) {
                return Err(invalid_array(
                    "an empty array cannot have a non-empty dimension",
                ));
            }
            return Self::empty(element_type);
        }
        if dimensions.is_empty() {
            return Err(invalid_array(
                "a non-empty array must have at least one dimension",
            ));
        }
        for dimension in &dimensions {
            let len = i32::try_from(dimension.len)
                .map_err(|_| invalid_array("array dimension length exceeds signed int32"))?;
            dimension
                .lower_bound
                .checked_add(len - 1)
                .ok_or_else(|| invalid_array("array dimension upper bound exceeds signed int32"))?;
        }
        let cardinality = dimensions.iter().try_fold(1_usize, |product, dimension| {
            let len = usize::try_from(dimension.len)
                .map_err(|_| invalid_array("array dimension length does not fit this platform"))?;
            product
                .checked_mul(len)
                .ok_or_else(|| invalid_array("array cardinality overflows"))
        })?;
        if cardinality != elements.len() {
            return Err(invalid_array(format!(
                "array dimensions describe {cardinality} elements but {} were provided",
                elements.len()
            )));
        }
        if let Some(value) = elements
            .iter()
            .find(|value| !value_matches_type(value, &element_type))
        {
            return Err(invalid_array(format!(
                "array element {value:?} does not match {element_type:?}"
            )));
        }
        Ok(Self {
            element_type,
            dimensions,
            elements,
        })
    }

    pub fn empty(element_type: DataType) -> Result<Self> {
        if matches!(element_type, DataType::Array(_)) {
            return Err(invalid_array("array elements cannot themselves be arrays"));
        }
        Ok(Self {
            element_type,
            dimensions: Vec::new(),
            elements: Vec::new(),
        })
    }

    #[must_use]
    pub fn element_type(&self) -> &DataType {
        &self.element_type
    }

    #[must_use]
    pub fn dimensions(&self) -> &[ArrayDimension] {
        &self.dimensions
    }

    #[must_use]
    pub fn elements(&self) -> &[Value] {
        &self.elements
    }

    #[must_use]
    pub fn cardinality(&self) -> usize {
        self.elements.len()
    }

    /// Resolve one SQL subscript per dimension to a row-major element offset.
    /// A dimensionality mismatch or out-of-bounds coordinate has no element.
    #[must_use]
    pub fn element_offset(&self, subscripts: &[i64]) -> Option<usize> {
        if subscripts.len() != self.dimensions.len() || self.dimensions.is_empty() {
            return None;
        }
        let mut offset = 0_usize;
        for (subscript, dimension) in subscripts.iter().zip(&self.dimensions) {
            let relative = i128::from(*subscript) - i128::from(dimension.lower_bound);
            let relative = usize::try_from(relative).ok()?;
            let len = usize::try_from(dimension.len).ok()?;
            if relative >= len {
                return None;
            }
            offset = offset.checked_mul(len)?.checked_add(relative)?;
        }
        Some(offset)
    }
}

impl<'de> Deserialize<'de> for SqlArray {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SerializedArray {
            element_type: DataType,
            dimensions: Vec<ArrayDimension>,
            elements: Vec<Value>,
        }

        let array = SerializedArray::deserialize(deserializer)?;
        Self::new(array.element_type, array.dimensions, array.elements)
            .map_err(serde::de::Error::custom)
    }
}

impl PartialEq for SqlArray {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SqlArray {}

impl PartialOrd for SqlArray {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SqlArray {
    fn cmp(&self, other: &Self) -> Ordering {
        self.element_type
            .cmp(&other.element_type)
            .then_with(|| self.elements.cmp(&other.elements))
            .then_with(|| self.elements.len().cmp(&other.elements.len()))
            .then_with(|| self.dimensions.len().cmp(&other.dimensions.len()))
            .then_with(|| {
                self.dimensions
                    .iter()
                    .map(|dimension| dimension.len)
                    .cmp(other.dimensions.iter().map(|dimension| dimension.len))
            })
            .then_with(|| {
                self.dimensions
                    .iter()
                    .map(|dimension| dimension.lower_bound)
                    .cmp(
                        other
                            .dimensions
                            .iter()
                            .map(|dimension| dimension.lower_bound),
                    )
            })
    }
}

impl std::hash::Hash for SqlArray {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.element_type.hash(state);
        self.elements.hash(state);
        self.dimensions.hash(state);
    }
}

/// Whether a logical value belongs to a semantic SQL type. NULL is accepted for
/// every type; column/expression nullability is enforced by the caller.
#[must_use]
pub fn value_matches_type(value: &Value, data_type: &DataType) -> bool {
    match (value, data_type) {
        (Value::Null, _) => true,
        (Value::Integer(_), DataType::Integer)
        | (Value::Text(_), DataType::Text)
        | (Value::Boolean(_), DataType::Boolean)
        | (Value::Date(_), DataType::Date)
        | (Value::Timestamp(_), DataType::Timestamp)
        | (Value::Time(_), DataType::Time)
        | (Value::TimestampTz(_), DataType::TimestampTz)
        | (Value::Interval(_), DataType::Interval)
        | (Value::Bytes(_), DataType::Bytea)
        | (Value::Uuid(_), DataType::Uuid)
        | (Value::Float(_), DataType::Double)
        | (Value::Real(_), DataType::Real)
        | (Value::Numeric(_), DataType::Numeric { .. }) => true,
        (Value::Array(array), DataType::Array(element_type)) => {
            array.element_type() == element_type.element_type()
        }
        _ => false,
    }
}

fn invalid_array(message: impl Into<String>) -> DbError {
    DbError::execute(SqlState::InvalidParameterValue, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_shape_and_element_type() {
        let array = SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(2, 1), ArrayDimension::new(2, -1)],
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(3),
                Value::Integer(4),
            ],
        )
        .unwrap();
        assert_eq!(array.cardinality(), 4);
        assert_eq!(array.element_offset(&[1, -1]), Some(0));
        assert_eq!(array.element_offset(&[2, 0]), Some(3));
        assert_eq!(array.element_offset(&[0, -1]), None);
        assert_eq!(array.element_offset(&[1]), None);

        assert!(
            SqlArray::new(
                DataType::Integer,
                vec![ArrayDimension::new(1, 1)],
                vec![Value::Text("wrong".to_string())],
            )
            .is_err()
        );
        assert!(
            SqlArray::new(
                DataType::Integer,
                vec![ArrayDimension::new(2, i32::MAX)],
                vec![Value::Integer(1), Value::Integer(2)],
            )
            .is_err()
        );
        assert!(
            SqlArray::new(
                DataType::Integer,
                vec![ArrayDimension::new(i32::MAX as u32 + 1, 1)],
                vec![Value::Integer(1)],
            )
            .is_err()
        );
    }

    #[test]
    fn empty_arrays_are_canonical() {
        let array =
            SqlArray::new(DataType::Text, vec![ArrayDimension::new(0, 42)], Vec::new()).unwrap();
        assert!(array.dimensions().is_empty());
        assert!(array.elements().is_empty());
    }

    #[test]
    fn ordering_compares_elements_before_dimensions() {
        let short = SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(1, 1)],
            vec![Value::Integer(2)],
        )
        .unwrap();
        let long = SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(2, 1)],
            vec![Value::Integer(1), Value::Integer(9)],
        )
        .unwrap();
        assert!(long < short, "row-major elements decide before shape");
    }
}
