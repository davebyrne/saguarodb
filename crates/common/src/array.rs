use std::cmp::Ordering;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{DataType, DbError, Result, SqlState, Value};

/// PostgreSQL-compatible maximum number of array dimensions.
pub const MAX_ARRAY_DIMENSIONS: usize = 6;
/// Practical allocation guard for one SQL array value.
pub const MAX_ARRAY_ELEMENTS: usize = 1_000_000;
const MAX_ARRAY_TEXT_LISTS: usize = MAX_ARRAY_DIMENSIONS * MAX_ARRAY_ELEMENTS + 1;

/// Parse PostgreSQL array text into dimensions and scalar text tokens. Scalar
/// conversion is deliberately left to the caller so COPY and protocol paths can
/// apply their own SQLSTATE boundary without introducing crate cycles.
pub fn parse_array_text_structure(
    text: &str,
) -> Result<(Vec<ArrayDimension>, Vec<Option<String>>)> {
    let mut parser = ArrayTextParser {
        bytes: text.as_bytes(),
        offset: 0,
        lists: 0,
        elements: 0,
    };
    parser.whitespace();
    let bounds = parser.bounds()?;
    let mut tokens = Vec::new();
    let shape = parser.list(0, &mut tokens)?;
    let lengths = shape.lengths[..shape.dimensions].to_vec();
    parser.whitespace();
    if parser.offset != parser.bytes.len() {
        return Err(invalid_array("array text has trailing input"));
    }
    if tokens.is_empty() {
        if lengths != [0] {
            return Err(invalid_array(
                "empty array text must have one empty brace level",
            ));
        }
        if !bounds.is_empty()
            && (bounds.len() != 1 || i64::from(bounds[0].1) - i64::from(bounds[0].0) + 1 != 0)
        {
            return Err(invalid_array("array bounds do not match empty contents"));
        }
    }
    if !bounds.is_empty() && bounds.len() != lengths.len() {
        return Err(invalid_array("array bounds do not match dimensionality"));
    }
    let dimensions = if tokens.is_empty() {
        Vec::new()
    } else {
        lengths
            .into_iter()
            .enumerate()
            .map(|(index, len)| {
                let lower = bounds.get(index).map_or(1, |bound| bound.0);
                if let Some((_, upper)) = bounds.get(index)
                    && i64::from(*upper) - i64::from(lower) + 1 != len as i64
                {
                    return Err(invalid_array("array bounds do not match contents"));
                }
                Ok(ArrayDimension::new(len as u32, lower))
            })
            .collect::<Result<Vec<_>>>()?
    };
    Ok((dimensions, tokens))
}

struct ArrayTextParser<'a> {
    bytes: &'a [u8],
    offset: usize,
    lists: usize,
    elements: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArrayTextShape {
    lengths: [usize; MAX_ARRAY_DIMENSIONS],
    dimensions: usize,
}

impl ArrayTextParser<'_> {
    fn bounds(&mut self) -> Result<Vec<(i32, i32)>> {
        let mut bounds = Vec::new();
        while self.bytes.get(self.offset) == Some(&b'[') {
            if bounds.len() >= MAX_ARRAY_DIMENSIONS {
                return Err(invalid_array("array text has too many dimensions"));
            }
            self.offset += 1;
            self.whitespace();
            let lower = self.integer()?;
            self.whitespace();
            if self.bytes.get(self.offset) != Some(&b':') {
                return Err(invalid_array("invalid array bounds"));
            }
            self.offset += 1;
            self.whitespace();
            let upper = self.integer()?;
            self.whitespace();
            if self.bytes.get(self.offset) != Some(&b']') {
                return Err(invalid_array("invalid array bounds"));
            }
            self.offset += 1;
            bounds.push((lower, upper));
        }
        if !bounds.is_empty() {
            self.whitespace();
            if self.bytes.get(self.offset) != Some(&b'=') {
                return Err(invalid_array("array bounds must be followed by '='"));
            }
            self.offset += 1;
            self.whitespace();
        }
        Ok(bounds)
    }

    fn integer(&mut self) -> Result<i32> {
        let start = self.offset;
        if matches!(self.bytes.get(self.offset), Some(b'+' | b'-')) {
            self.offset += 1;
        }
        while self.bytes.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        if self.offset == start {
            return Err(invalid_array("invalid array bound"));
        }
        std::str::from_utf8(&self.bytes[start..self.offset])
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| invalid_array("invalid array bound"))
    }

    fn whitespace(&mut self) {
        while self
            .bytes
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }

    fn list(&mut self, depth: usize, tokens: &mut Vec<Option<String>>) -> Result<ArrayTextShape> {
        self.lists += 1;
        if self.lists > MAX_ARRAY_TEXT_LISTS {
            return Err(invalid_array("array text has too many nested lists"));
        }
        if depth >= MAX_ARRAY_DIMENSIONS {
            return Err(invalid_array("array text has too many dimensions"));
        }
        self.whitespace();
        if self.bytes.get(self.offset) != Some(&b'{') {
            return Err(invalid_array("array text must start with '{'"));
        }
        self.offset += 1;
        self.whitespace();
        if self.bytes.get(self.offset) == Some(&b'}') {
            self.offset += 1;
            if depth != 0 {
                return Err(invalid_array("nested empty arrays are not supported"));
            }
            let mut lengths = [0; MAX_ARRAY_DIMENSIONS];
            lengths[0] = 0;
            return Ok(ArrayTextShape {
                lengths,
                dimensions: 1,
            });
        }
        let mut item_count = 0;
        let mut nested_shape: Option<ArrayTextShape> = None;
        let mut scalar_items = false;
        loop {
            self.whitespace();
            if self.bytes.get(self.offset) == Some(&b'{') {
                if scalar_items {
                    return Err(invalid_array("array mixes nested and scalar elements"));
                }
                let shape = self.list(depth + 1, tokens)?;
                if let Some(expected) = &nested_shape {
                    if expected != &shape {
                        return Err(invalid_array("multidimensional array is not rectangular"));
                    }
                } else {
                    nested_shape = Some(shape);
                }
            } else {
                if nested_shape.is_some() {
                    return Err(invalid_array("array mixes nested and scalar elements"));
                }
                scalar_items = true;
                tokens.push(self.element()?);
            }
            item_count += 1;
            self.whitespace();
            match self.bytes.get(self.offset) {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    break;
                }
                _ => return Err(invalid_array("invalid array text syntax")),
            }
        }
        let mut lengths = [0; MAX_ARRAY_DIMENSIONS];
        lengths[0] = item_count;
        let dimensions = nested_shape.map_or(1, |nested| {
            lengths[1..=nested.dimensions].copy_from_slice(&nested.lengths[..nested.dimensions]);
            nested.dimensions + 1
        });
        Ok(ArrayTextShape {
            lengths,
            dimensions,
        })
    }

    fn element(&mut self) -> Result<Option<String>> {
        self.elements += 1;
        if self.elements > MAX_ARRAY_ELEMENTS {
            return Err(invalid_array("array text has too many elements"));
        }
        let quoted = self.bytes.get(self.offset) == Some(&b'"');
        if quoted {
            self.offset += 1;
        }
        let mut value: Vec<(u8, bool)> = Vec::new();
        loop {
            let Some(&byte) = self.bytes.get(self.offset) else {
                return Err(invalid_array("unterminated array element"));
            };
            if byte == b'\\' {
                self.offset += 1;
                let Some(&escaped) = self.bytes.get(self.offset) else {
                    return Err(invalid_array("unterminated array escape"));
                };
                value.push((escaped, true));
                self.offset += 1;
                continue;
            }
            if quoted && byte == b'"' {
                self.offset += 1;
                break;
            }
            if !quoted && matches!(byte, b',' | b'}') {
                break;
            }
            if !quoted && matches!(byte, b'"' | b'{') {
                return Err(invalid_array(
                    "array element contains an unescaped special character",
                ));
            }
            value.push((byte, false));
            self.offset += 1;
        }
        let (mut start, mut end) = (0, value.len());
        if !quoted {
            while start < end && !value[start].1 && value[start].0.is_ascii_whitespace() {
                start += 1;
            }
            while end > start && !value[end - 1].1 && value[end - 1].0.is_ascii_whitespace() {
                end -= 1;
            }
        }
        let text = String::from_utf8(value[start..end].iter().map(|(byte, _)| *byte).collect())
            .map_err(|_| invalid_array("array element is not UTF-8"))?;
        if !quoted && text.is_empty() {
            return Err(invalid_array("array element must not be empty"));
        }
        Ok(if !quoted && text.eq_ignore_ascii_case("NULL") {
            None
        } else {
            Some(text)
        })
    }
}

/// Format an array's structure as PostgreSQL array text. The callback supplies
/// the text representation of each non-null scalar element.
pub fn format_array_text_structure<E>(
    array: &SqlArray,
    mut format_element: impl FnMut(&Value) -> std::result::Result<String, E>,
) -> std::result::Result<String, E> {
    if array.elements().is_empty() {
        return Ok("{}".to_string());
    }
    let mut output = String::new();
    if array
        .dimensions()
        .iter()
        .any(|dimension| dimension.lower_bound() != 1)
    {
        for dimension in array.dimensions() {
            let upper = i64::from(dimension.lower_bound()) + i64::from(dimension.len()) - 1;
            output.push_str(&format!("[{}:{upper}]", dimension.lower_bound()));
        }
        output.push('=');
    }
    let mut element_index = 0;
    format_array_text_level(
        &mut output,
        array,
        0,
        &mut element_index,
        &mut format_element,
    )?;
    Ok(output)
}

fn format_array_text_level<E>(
    output: &mut String,
    array: &SqlArray,
    depth: usize,
    element_index: &mut usize,
    format_element: &mut impl FnMut(&Value) -> std::result::Result<String, E>,
) -> std::result::Result<(), E> {
    output.push('{');
    for index in 0..array.dimensions()[depth].len() as usize {
        if index != 0 {
            output.push(',');
        }
        if depth + 1 < array.dimensions().len() {
            format_array_text_level(output, array, depth + 1, element_index, format_element)?;
        } else {
            let value = &array.elements()[*element_index];
            *element_index += 1;
            if matches!(value, Value::Null) {
                output.push_str("NULL");
            } else {
                write_array_text_element(output, &format_element(value)?);
            }
        }
    }
    output.push('}');
    Ok(())
}

fn write_array_text_element(output: &mut String, text: &str) {
    let quote = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|ch| ch.is_ascii_whitespace() || matches!(ch, '{' | '}' | ',' | '"' | '\\'));
    if quote {
        output.push('"');
    }
    for ch in text.chars() {
        if matches!(ch, '"' | '\\') {
            output.push('\\');
        }
        output.push(ch);
    }
    if quote {
        output.push('"');
    }
}

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
            #[serde(deserialize_with = "deserialize_dimensions")]
            dimensions: Vec<ArrayDimension>,
            #[serde(deserialize_with = "deserialize_elements")]
            elements: Vec<Value>,
        }

        fn deserialize_dimensions<'de, D>(
            deserializer: D,
        ) -> std::result::Result<Vec<ArrayDimension>, D::Error>
        where
            D: Deserializer<'de>,
        {
            crate::durable::deserialize_bounded_vec_with_limit::<
                D,
                ArrayDimension,
                MAX_ARRAY_DIMENSIONS,
            >(deserializer)
        }

        fn deserialize_elements<'de, D>(
            deserializer: D,
        ) -> std::result::Result<Vec<Value>, D::Error>
        where
            D: Deserializer<'de>,
        {
            crate::durable::deserialize_bounded_vec_with_limit::<D, Value, MAX_ARRAY_ELEMENTS>(
                deserializer,
            )
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
    fn parses_array_text_structure_with_quotes_nulls_bounds_and_shape() {
        let (dimensions, elements) =
            parse_array_text_structure(r#"[-1:0][2:3]={{"a,b",NULL},{"NULL","x\\y"}}"#).unwrap();
        assert_eq!(
            dimensions,
            vec![ArrayDimension::new(2, -1), ArrayDimension::new(2, 2)]
        );
        assert_eq!(
            elements,
            vec![
                Some("a,b".to_string()),
                None,
                Some("NULL".to_string()),
                Some("x\\y".to_string())
            ]
        );
        assert!(parse_array_text_structure("{{1},{2,3}}").is_err());
    }

    #[test]
    fn array_text_parser_rejects_excess_dimensions_and_malformed_empty_tokens() {
        for text in [
            "{{{{{{{1}}}}}}}",
            "[1:1][1:1][1:1][1:1][1:1][1:1][1:1]={{{{{{{1}}}}}}}",
            "{,}",
            "{1,}",
            "{a\"b}",
            "{a{b}",
            "{{}}",
            "[1:2]={}",
        ] {
            assert!(
                parse_array_text_structure(text).is_err(),
                "accepted malformed array text: {text}"
            );
        }
        assert!(parse_array_text_structure("[ 1 : 0 ] = {}").is_ok());
        assert_eq!(
            parse_array_text_structure("{ }").unwrap().1,
            Vec::<Option<String>>::new()
        );
        assert_eq!(
            parse_array_text_structure("{\\ }").unwrap().1,
            vec![Some(" ".to_string())]
        );

        let mut at_element_limit = ArrayTextParser {
            bytes: b"1}",
            offset: 0,
            lists: 0,
            elements: MAX_ARRAY_ELEMENTS - 1,
        };
        assert_eq!(at_element_limit.element().unwrap(), Some("1".to_string()));
        let mut beyond_element_limit = ArrayTextParser {
            bytes: b"1}",
            offset: 0,
            lists: 0,
            elements: MAX_ARRAY_ELEMENTS,
        };
        assert_eq!(
            beyond_element_limit.element().unwrap_err().message,
            "array text has too many elements"
        );
        let mut beyond_list_limit = ArrayTextParser {
            bytes: b"{}",
            offset: 0,
            lists: MAX_ARRAY_TEXT_LISTS,
            elements: 0,
        };
        assert_eq!(
            beyond_list_limit
                .list(0, &mut Vec::new())
                .unwrap_err()
                .message,
            "array text has too many nested lists"
        );
    }

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
