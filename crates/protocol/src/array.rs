use common::{
    ArrayDimension, DbError, MAX_ARRAY_DIMENSIONS, MAX_ARRAY_ELEMENTS, PgType, Result, SqlArray,
    SqlState, Value, format_array_text_structure, parse_array_text_structure,
};

pub(crate) fn encode(array: &SqlArray, element_type: &PgType, binary: bool) -> Result<Vec<u8>> {
    if array.element_type() != &element_type.data_type() {
        return Err(array_error(
            "array element type does not match its wire type",
        ));
    }
    if binary {
        encode_binary(array, element_type)
    } else {
        encode_text(array, element_type)
    }
}

pub(crate) fn decode(bytes: &[u8], element_type: &PgType, binary: bool) -> Result<SqlArray> {
    if binary {
        decode_binary(bytes, element_type).map_err(|error| {
            DbError::protocol(SqlState::InvalidBinaryRepresentation, error.message)
        })
    } else {
        decode_text(bytes, element_type)
    }
}

fn encode_binary(array: &SqlArray, element_type: &PgType) -> Result<Vec<u8>> {
    let ndim = i32::try_from(array.dimensions().len())
        .map_err(|_| array_error("array has too many dimensions"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&ndim.to_be_bytes());
    bytes.extend_from_slice(
        &i32::from(
            array
                .elements()
                .iter()
                .any(|value| matches!(value, Value::Null)),
        )
        .to_be_bytes(),
    );
    bytes.extend_from_slice(&element_type.oid().to_be_bytes());
    for dimension in array.dimensions() {
        bytes.extend_from_slice(
            &i32::try_from(dimension.len())
                .map_err(|_| array_error("array dimension exceeds signed int32"))?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&dimension.lower_bound().to_be_bytes());
    }
    for value in array.elements() {
        match crate::codec::encode_value_with_type(value, element_type, 1)? {
            None => bytes.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(payload) => {
                let len = i32::try_from(payload.len())
                    .map_err(|_| array_error("array element is too large"))?;
                bytes.extend_from_slice(&len.to_be_bytes());
                bytes.extend_from_slice(&payload);
            }
        }
    }
    Ok(bytes)
}

fn decode_binary(bytes: &[u8], element_type: &PgType) -> Result<SqlArray> {
    let mut cursor = Cursor::new(bytes);
    let ndim = cursor.i32()?;
    if !(0..=MAX_ARRAY_DIMENSIONS as i32).contains(&ndim) {
        return Err(array_error("invalid binary array dimension count"));
    }
    let has_null = cursor.i32()?;
    if !matches!(has_null, 0 | 1) {
        return Err(array_error("invalid binary array null flag"));
    }
    let element_oid = cursor.i32()?;
    if element_oid != element_type.oid() {
        return Err(array_error(
            "binary array element OID does not match parameter type",
        ));
    }
    let mut dimensions = Vec::with_capacity(ndim as usize);
    let mut cardinality = usize::from(ndim == 0);
    for _ in 0..ndim {
        let len = cursor.i32()?;
        if len < 0 {
            return Err(array_error("binary array dimension length is negative"));
        }
        let len = len as usize;
        cardinality = if dimensions.is_empty() {
            len
        } else {
            cardinality
                .checked_mul(len)
                .ok_or_else(|| array_error("binary array cardinality overflows"))?
        };
        if cardinality > MAX_ARRAY_ELEMENTS {
            return Err(array_error("binary array has too many elements"));
        }
        dimensions.push(ArrayDimension::new(len as u32, cursor.i32()?));
    }
    if ndim == 0 {
        cardinality = 0;
    } else if cardinality == 0 {
        return Err(array_error("empty binary array must have zero dimensions"));
    }
    let mut elements = Vec::with_capacity(cardinality);
    let mut saw_null = false;
    for _ in 0..cardinality {
        let len = cursor.i32()?;
        if len == -1 {
            saw_null = true;
            elements.push(Value::Null);
        } else {
            let len = usize::try_from(len)
                .map_err(|_| array_error("binary array element length is negative"))?;
            validate_binary_element_width(element_type, len)?;
            elements.push(crate::codec::decode_value_with_type(
                cursor.take(len)?,
                element_type,
                1,
            )?);
        }
    }
    if cursor.remaining() != 0 {
        return Err(array_error("binary array has trailing bytes"));
    }
    if saw_null != (has_null == 1) {
        return Err(array_error(
            "binary array null flag does not match its elements",
        ));
    }
    SqlArray::new(element_type.data_type(), dimensions, elements)
        .map_err(|error| array_error(error.message))
}

fn encode_text(array: &SqlArray, element_type: &PgType) -> Result<Vec<u8>> {
    format_array_text_structure(array, |value| {
        let bytes = crate::codec::encode_value_with_type(value, element_type, 0)?
            .ok_or_else(|| array_error("non-null array element encoded as NULL"))?;
        std::str::from_utf8(&bytes)
            .map(str::to_owned)
            .map_err(|_| array_error("array element text is not UTF-8"))
    })
    .map(String::into_bytes)
}

fn decode_text(bytes: &[u8], element_type: &PgType) -> Result<SqlArray> {
    let text = std::str::from_utf8(bytes).map_err(|_| array_error("array text is not UTF-8"))?;
    let (dimensions, tokens) =
        parse_array_text_structure(text).map_err(|error| array_error(error.message))?;
    if tokens.is_empty() {
        return SqlArray::empty(element_type.data_type())
            .map_err(|error| array_error(error.message));
    }
    let elements = tokens
        .into_iter()
        .map(|token| match token {
            None => Ok(Value::Null),
            Some(token) => crate::codec::decode_value_with_type(token.as_bytes(), element_type, 0)
                .map_err(|error| {
                    if error.code == SqlState::SyntaxError {
                        array_error(error.message)
                    } else {
                        error
                    }
                }),
        })
        .collect::<Result<Vec<_>>>()?;
    SqlArray::new(element_type.data_type(), dimensions, elements)
        .map_err(|error| array_error(error.message))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn i32(&mut self) -> Result<i32> {
        let bytes = self
            .take(4)?
            .try_into()
            .map_err(|_| array_error("binary array fixed-width field has the wrong length"))?;
        Ok(i32::from_be_bytes(bytes))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| array_error("binary array length overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| array_error("binary array is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

fn array_error(message: impl Into<String>) -> DbError {
    DbError::protocol(SqlState::InvalidTextRepresentation, message)
}

fn validate_binary_element_width(element_type: &PgType, len: usize) -> Result<()> {
    let expected = match element_type {
        PgType::Int2 => Some(2),
        PgType::Int4 | PgType::Oid => Some(4),
        PgType::Int8 => Some(8),
        _ => None,
    };
    if expected.is_some_and(|expected| expected != len) {
        Err(array_error(format!(
            "binary {} array element has invalid length {len}",
            element_type.format_type_name()
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::{ArrayDimension, DataType, PgType, SqlArray, SqlState, Value};

    use super::{decode, encode};

    #[test]
    fn text_round_trip_preserves_multidimensional_bounds_and_nulls() {
        let array = SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(2, -1), ArrayDimension::new(2, 3)],
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(3),
                Value::Integer(4),
            ],
        )
        .unwrap();
        let encoded = encode(&array, &PgType::Int4, false).unwrap();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "[-1:0][3:4]={{1,NULL},{3,4}}"
        );
        assert_eq!(decode(&encoded, &PgType::Int4, false).unwrap(), array);
    }

    #[test]
    fn text_codec_distinguishes_null_and_escaped_text() {
        let decoded = decode(
            br#"{"NULL",NULL,"a,b","quote\"slash\\"}"#,
            &PgType::Text,
            false,
        )
        .unwrap();
        assert_eq!(
            decoded.elements(),
            &[
                Value::Text("NULL".to_string()),
                Value::Null,
                Value::Text("a,b".to_string()),
                Value::Text("quote\"slash\\".to_string()),
            ]
        );
        let encoded = encode(&decoded, &PgType::Text, false).unwrap();
        assert_eq!(decode(&encoded, &PgType::Text, false).unwrap(), decoded);

        let spaces = decode(br"{\ foo,foo\ ,  bar  }", &PgType::Text, false).unwrap();
        assert_eq!(
            spaces.elements(),
            &[
                Value::Text(" foo".to_string()),
                Value::Text("foo ".to_string()),
                Value::Text("bar".to_string()),
            ]
        );
    }

    #[test]
    fn binary_round_trip_uses_declared_element_oid_and_width() {
        let array = SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(3, 1)],
            vec![Value::Integer(1), Value::Null, Value::Integer(-2)],
        )
        .unwrap();
        let encoded = encode(&array, &PgType::Int4, true).unwrap();
        assert_eq!(&encoded[8..12], &23_i32.to_be_bytes());
        assert_eq!(decode(&encoded, &PgType::Int4, true).unwrap(), array);

        let mut wrong_oid = encoded.clone();
        wrong_oid[8..12].copy_from_slice(&20_i32.to_be_bytes());
        assert_eq!(
            decode(&wrong_oid, &PgType::Int4, true).unwrap_err().code,
            SqlState::InvalidBinaryRepresentation
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode(&trailing, &PgType::Int4, true).is_err());

        let one_int4 = SqlArray::new(
            DataType::Integer,
            vec![ArrayDimension::new(1, 1)],
            vec![Value::Integer(1)],
        )
        .unwrap();
        let mut wrong_width = encode(&one_int4, &PgType::Int4, true).unwrap();
        wrong_width[20..24].copy_from_slice(&8_i32.to_be_bytes());
        wrong_width.extend_from_slice(&[0; 4]);
        assert_eq!(
            decode(&wrong_width, &PgType::Int4, true).unwrap_err().code,
            SqlState::InvalidBinaryRepresentation
        );
    }

    #[test]
    fn rejects_ragged_and_overdeep_text_arrays() {
        assert_eq!(
            decode(b"{{1},{2,3}}", &PgType::Int4, false)
                .unwrap_err()
                .code,
            SqlState::InvalidTextRepresentation
        );
        assert!(decode(b"{{{{{{{1}}}}}}}", &PgType::Int4, false).is_err());
        assert!(decode(b"{{}}", &PgType::Int4, false).is_err());
        assert!(decode(b"[1:2]={}", &PgType::Int4, false).is_err());
        assert!(decode(b"[1:0]={}", &PgType::Int4, false).is_ok());
        assert_eq!(
            decode(b"{not-an-int}", &PgType::Int4, false)
                .unwrap_err()
                .code,
            SqlState::InvalidTextRepresentation
        );
        assert_eq!(
            decode(b"{32768}", &PgType::Int2, false).unwrap_err().code,
            SqlState::NumericValueOutOfRange
        );
        assert_eq!(
            decode(b"{2147483648}", &PgType::Int4, false)
                .unwrap_err()
                .code,
            SqlState::NumericValueOutOfRange
        );
    }

    #[test]
    fn timestamp_epoch_arithmetic_rejects_overflow() {
        let array = SqlArray::new(
            DataType::Timestamp,
            vec![ArrayDimension::new(1, 1)],
            vec![Value::Timestamp(0)],
        )
        .unwrap();
        let mut encoded = encode(&array, &PgType::Timestamp, true).unwrap();
        encoded[24..32].copy_from_slice(&i64::MAX.to_be_bytes());
        assert_eq!(
            decode(&encoded, &PgType::Timestamp, true).unwrap_err().code,
            SqlState::InvalidBinaryRepresentation
        );

        let extreme = SqlArray::new(
            DataType::TimestampTz,
            vec![ArrayDimension::new(1, 1)],
            vec![Value::TimestampTz(i64::MIN)],
        )
        .unwrap();
        assert_eq!(
            encode(&extreme, &PgType::Timestamptz, true)
                .unwrap_err()
                .code,
            SqlState::NumericValueOutOfRange
        );

        let extreme_date = SqlArray::new(
            DataType::Date,
            vec![ArrayDimension::new(1, 1)],
            vec![Value::Date(i64::MIN)],
        )
        .unwrap();
        assert_eq!(
            encode(&extreme_date, &PgType::Date, true).unwrap_err().code,
            SqlState::NumericValueOutOfRange
        );
    }

    #[test]
    fn every_scalar_element_family_round_trips_in_both_formats() {
        let cases = vec![
            (DataType::Integer, PgType::Int2, Value::Integer(-7)),
            (DataType::Integer, PgType::Int4, Value::Integer(8)),
            (DataType::Integer, PgType::Int8, Value::Integer(9)),
            (
                DataType::Integer,
                PgType::Oid,
                Value::Integer(4_000_000_000),
            ),
            (DataType::Text, PgType::Text, Value::Text("a,b".to_string())),
            (DataType::Boolean, PgType::Bool, Value::Boolean(true)),
            (DataType::Date, PgType::Date, Value::Date(20_000)),
            (
                DataType::Timestamp,
                PgType::Timestamp,
                Value::Timestamp(1_700_000_000_000_000),
            ),
            (DataType::Time, PgType::Time, Value::Time(12_345_678)),
            (
                DataType::TimestampTz,
                PgType::Timestamptz,
                Value::TimestampTz(1_700_000_000_000_000),
            ),
            (
                DataType::Interval,
                PgType::Interval,
                Value::Interval(common::Interval::new(1, 2, 3)),
            ),
            (
                DataType::Bytea,
                PgType::Bytea,
                Value::Bytes(vec![0, 1, 255]),
            ),
            (DataType::Uuid, PgType::Uuid, Value::Uuid([7; 16])),
            (DataType::Double, PgType::Float8, Value::Float(1.25.into())),
            (DataType::Real, PgType::Float4, Value::Real(2.5.into())),
            (
                DataType::Numeric {
                    precision: Some(8),
                    scale: 2,
                },
                PgType::Numeric {
                    precision: Some(8),
                    scale: 2,
                },
                Value::Numeric(common::numeric::parse_numeric("12.34").unwrap()),
            ),
        ];
        for (data_type, pg_type, value) in cases {
            let array =
                SqlArray::new(data_type, vec![ArrayDimension::new(1, 1)], vec![value]).unwrap();
            for binary in [false, true] {
                let encoded = encode(&array, &pg_type, binary).unwrap();
                assert_eq!(
                    decode(&encoded, &pg_type, binary).unwrap(),
                    array,
                    "{pg_type:?} binary={binary}"
                );
            }
        }
    }
}
