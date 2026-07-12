use serde::{Deserialize, Deserializer, Serialize};

use crate::DataType;

/// Bytes PostgreSQL adds to a type modifier before storing it in `atttypmod`
/// (`VARHDRSZ`). A `varchar(n)` reports `n + 4`; a client subtracts it back.
const VARHDRSZ: i32 = 4;

/// The PostgreSQL wire identity of a column or value: the type OID, its on-wire
/// length (`typlen`), and its type modifier (`atttypmod`), as reported in
/// `RowDescription` and `ParameterDescription`.
///
/// This is presentational metadata only. It never participates in storage,
/// comparison, or type checking — those use [`DataType`]. `PgType` exists so the
/// width, character-kind, and length distinctions that `DataType` intentionally
/// collapses (every integer to a single 64-bit type, every character type to
/// `TEXT`) are still reported accurately to clients. Deliberately has no
/// `Default`: a wire type is only meaningful relative to a `DataType`, so the
/// durable fallback is an `Option<PgType>` resolved through [`PgType::from`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum PgType {
    Int2,
    Int4,
    Int8,
    Oid,
    Bool,
    Float4,
    Float8,
    Numeric {
        precision: Option<u32>,
        scale: u32,
    },
    Text,
    /// `varchar(n)`; `None` is unbounded `VARCHAR`.
    Varchar(Option<u32>),
    /// `char(n)` / `character(n)`; `None` is a bare `CHAR` with no length.
    Bpchar(Option<u32>),
    Bytea,
    Uuid,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Interval,
    OidVector,
    Int2Vector,
    OidArray,
    Int2Array,
    /// A real PostgreSQL array wire type. The pre-existing `Int2Array` and
    /// `OidArray` variants remain as durable aliases for their element types.
    Array(Box<PgType>),
}

impl PgType {
    /// Decode a PostgreSQL type OID plus type modifier into the presentational
    /// wire type SaguaroDB exposes. Unsupported OIDs return `None`.
    pub fn from_oid_typmod(oid: i64, typmod: i64) -> Option<Self> {
        let typmod = i32::try_from(typmod).ok()?;
        Some(match oid {
            16 => PgType::Bool,
            17 => PgType::Bytea,
            20 => PgType::Int8,
            21 => PgType::Int2,
            22 => PgType::Int2Vector,
            23 => PgType::Int4,
            25 => PgType::Text,
            26 => PgType::Oid,
            30 => PgType::OidVector,
            700 => PgType::Float4,
            701 => PgType::Float8,
            1005 => PgType::Int2Array,
            1000 => PgType::Array(Box::new(PgType::Bool)),
            1001 => PgType::Array(Box::new(PgType::Bytea)),
            1007 => PgType::Array(Box::new(PgType::Int4)),
            1009 => PgType::Array(Box::new(PgType::Text)),
            1014 => PgType::Array(Box::new(PgType::Bpchar(decode_length_typmod(typmod)))),
            1015 => PgType::Array(Box::new(PgType::Varchar(decode_length_typmod(typmod)))),
            1016 => PgType::Array(Box::new(PgType::Int8)),
            1021 => PgType::Array(Box::new(PgType::Float4)),
            1022 => PgType::Array(Box::new(PgType::Float8)),
            1028 => PgType::OidArray,
            1115 => PgType::Array(Box::new(PgType::Timestamp)),
            1182 => PgType::Array(Box::new(PgType::Date)),
            1183 => PgType::Array(Box::new(PgType::Time)),
            1185 => PgType::Array(Box::new(PgType::Timestamptz)),
            1187 => PgType::Array(Box::new(PgType::Interval)),
            1231 => {
                let (precision, scale) = decode_numeric_typmod(typmod);
                PgType::Array(Box::new(PgType::Numeric { precision, scale }))
            }
            1042 => PgType::Bpchar(decode_length_typmod(typmod)),
            1043 => PgType::Varchar(decode_length_typmod(typmod)),
            1082 => PgType::Date,
            1083 => PgType::Time,
            1114 => PgType::Timestamp,
            1184 => PgType::Timestamptz,
            1186 => PgType::Interval,
            1700 => {
                let (precision, scale) = decode_numeric_typmod(typmod);
                PgType::Numeric { precision, scale }
            }
            2950 => PgType::Uuid,
            2951 => PgType::Array(Box::new(PgType::Uuid)),
            _ => return None,
        })
    }

    /// Resolve a PostgreSQL type spelling commonly used in catalog probes to its
    /// type OID. This is intentionally limited to SaguaroDB's exposed type set.
    pub fn oid_for_name(name: &str) -> Option<i64> {
        let name = name.trim().to_ascii_lowercase();
        let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        Some(match name {
            "bool" | "boolean" => 16,
            "bytea" => 17,
            "int8" | "bigint" => 20,
            "int2" | "smallint" => 21,
            "int4" | "integer" | "int" => 23,
            "text" => 25,
            "oid" => 26,
            "oidvector" => 30,
            "int2vector" => 22,
            "int2[]" | "smallint[]" | "_int2" => 1005,
            "oid[]" | "_oid" => 1028,
            "bool[]" | "boolean[]" | "_bool" => 1000,
            "bytea[]" | "_bytea" => 1001,
            "int4[]" | "integer[]" | "int[]" | "_int4" => 1007,
            "text[]" | "_text" => 1009,
            "bpchar[]" | "char[]" | "character[]" | "_bpchar" => 1014,
            "varchar[]" | "character varying[]" | "_varchar" => 1015,
            "int8[]" | "bigint[]" | "_int8" => 1016,
            "float4[]" | "real[]" | "_float4" => 1021,
            "float8[]" | "double precision[]" | "float[]" | "_float8" => 1022,
            "timestamp[]" | "timestamp without time zone[]" | "_timestamp" => 1115,
            "date[]" | "_date" => 1182,
            "time[]" | "time without time zone[]" | "_time" => 1183,
            "timestamptz[]" | "timestamp with time zone[]" | "_timestamptz" => 1185,
            "interval[]" | "_interval" => 1187,
            "numeric[]" | "decimal[]" | "_numeric" => 1231,
            "uuid[]" | "_uuid" => 2951,
            "float4" | "real" => 700,
            "float8" | "double precision" | "float" => 701,
            "bpchar" | "char" | "character" => 1042,
            "varchar" | "character varying" => 1043,
            "date" => 1082,
            "time" | "time without time zone" => 1083,
            "timestamp" | "timestamp without time zone" => 1114,
            "timestamptz" | "timestamp with time zone" => 1184,
            "interval" => 1186,
            "numeric" | "decimal" => 1700,
            "uuid" => 2950,
            _ => return None,
        })
    }

    /// PostgreSQL-style SQL display name used by `format_type`.
    pub fn format_type_name(&self) -> String {
        match self {
            PgType::Int2 => "smallint".to_string(),
            PgType::Int4 => "integer".to_string(),
            PgType::Int8 => "bigint".to_string(),
            PgType::Oid => "oid".to_string(),
            PgType::Bool => "boolean".to_string(),
            PgType::Float4 => "real".to_string(),
            PgType::Float8 => "double precision".to_string(),
            PgType::Numeric {
                precision: Some(precision),
                scale,
            } if *scale == 0 => format!("numeric({precision})"),
            PgType::Numeric {
                precision: Some(precision),
                scale,
            } => format!("numeric({precision},{scale})"),
            PgType::Numeric {
                precision: None, ..
            } => "numeric".to_string(),
            PgType::Text => "text".to_string(),
            PgType::Varchar(Some(length)) => format!("character varying({length})"),
            PgType::Varchar(None) => "character varying".to_string(),
            PgType::Bpchar(Some(length)) => format!("character({length})"),
            PgType::Bpchar(None) => "character".to_string(),
            PgType::Bytea => "bytea".to_string(),
            PgType::Uuid => "uuid".to_string(),
            PgType::Date => "date".to_string(),
            PgType::Time => "time without time zone".to_string(),
            PgType::Timestamp => "timestamp without time zone".to_string(),
            PgType::Timestamptz => "timestamp with time zone".to_string(),
            PgType::Interval => "interval".to_string(),
            PgType::OidVector => "oidvector".to_string(),
            PgType::Int2Vector => "int2vector".to_string(),
            PgType::OidArray => "oid[]".to_string(),
            PgType::Int2Array => "smallint[]".to_string(),
            PgType::Array(element) => format!("{}[]", element.format_type_name()),
        }
    }

    /// The PostgreSQL type OID reported on the wire.
    pub fn oid(&self) -> i32 {
        match self {
            PgType::Int2 => 21,
            PgType::Int4 => 23,
            PgType::Int8 => 20,
            PgType::Oid => 26,
            PgType::Bool => 16,
            PgType::Float4 => 700,
            PgType::Float8 => 701,
            PgType::Numeric { .. } => 1700,
            PgType::Text => 25,
            PgType::Varchar(_) => 1043,
            PgType::Bpchar(_) => 1042,
            PgType::Bytea => 17,
            PgType::Uuid => 2950,
            PgType::Date => 1082,
            PgType::Time => 1083,
            PgType::Timestamp => 1114,
            PgType::Timestamptz => 1184,
            PgType::Interval => 1186,
            PgType::OidVector => 30,
            PgType::Int2Vector => 22,
            PgType::OidArray => 1028,
            PgType::Int2Array => 1005,
            PgType::Array(element) => array_oid(element)
                .expect("PgType::Array must contain a supported scalar element type"),
        }
    }

    /// The fixed on-wire byte length (`typlen`), or `-1` for a variable-length type.
    pub fn typlen(&self) -> i16 {
        match self {
            PgType::Bool => 1,
            PgType::Int2 => 2,
            PgType::Int4 | PgType::Oid | PgType::Float4 | PgType::Date => 4,
            PgType::Int8
            | PgType::Float8
            | PgType::Time
            | PgType::Timestamp
            | PgType::Timestamptz => 8,
            PgType::Uuid | PgType::Interval => 16,
            PgType::Numeric { .. }
            | PgType::Text
            | PgType::Varchar(_)
            | PgType::Bpchar(_)
            | PgType::Bytea
            | PgType::OidVector
            | PgType::Int2Vector
            | PgType::OidArray
            | PgType::Int2Array => -1,
            PgType::Array(_) => -1,
        }
    }

    /// The PostgreSQL type modifier (`atttypmod`), or `-1` when unconstrained.
    /// Encodes the declared length of `varchar(n)`/`char(n)` and the
    /// precision/scale of `numeric(p, s)`, matching how clients decode it.
    pub fn typmod(&self) -> i32 {
        // Compute the modifier in `i64`, then fold once to `i32`. A declared
        // length/precision is normally tiny (the parser caps precision to 1..=28,
        // though not length), but `PgType` is a public value object with public
        // fields, so a value too large to encode degrades to "unconstrained"
        // (`-1`) rather than panicking or emitting a garbage negative modifier.
        let modifier: i64 = match self {
            PgType::Varchar(Some(len)) | PgType::Bpchar(Some(len)) => {
                i64::from(*len) + i64::from(VARHDRSZ)
            }
            PgType::Numeric {
                precision: Some(precision),
                scale,
            } => {
                // Numeric typmod packs precision in the high 16 bits and scale in
                // the low 16, so each must fit a 16-bit field to be encodable;
                // otherwise degrade to "unconstrained" like the other arms.
                let (Ok(precision), Ok(scale)) = (u16::try_from(*precision), u16::try_from(*scale))
                else {
                    return -1;
                };
                ((i64::from(precision) << 16) | i64::from(scale)) + i64::from(VARHDRSZ)
            }
            PgType::Array(element) => return element.typmod(),
            _ => return -1,
        };
        i32::try_from(modifier).unwrap_or(-1)
    }

    /// The storage/semantic [`DataType`] this wire type refines. The distinctions
    /// `DataType` collapses are dropped here: every integer width maps to
    /// `Integer` and every character kind to `Text`; all other types are 1:1.
    /// This is the inverse of [`PgType::from`] on the collapsed families, so
    /// `PgType::from(&dt).data_type() == dt` for every `DataType`.
    pub fn data_type(&self) -> DataType {
        match self {
            PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Oid => DataType::Integer,
            PgType::Bool => DataType::Boolean,
            PgType::Float4 => DataType::Real,
            PgType::Float8 => DataType::Double,
            PgType::Numeric { precision, scale } => DataType::Numeric {
                precision: *precision,
                scale: *scale,
            },
            PgType::Text
            | PgType::Varchar(_)
            | PgType::Bpchar(_)
            | PgType::OidVector
            | PgType::Int2Vector
            | PgType::OidArray
            | PgType::Int2Array => DataType::Text,
            PgType::Array(element) => DataType::Array(Box::new(element.data_type())),
            PgType::Bytea => DataType::Bytea,
            PgType::Uuid => DataType::Uuid,
            PgType::Date => DataType::Date,
            PgType::Time => DataType::Time,
            PgType::Timestamp => DataType::Timestamp,
            PgType::Timestamptz => DataType::TimestampTz,
            PgType::Interval => DataType::Interval,
        }
    }

    /// If `value` does not fit this type's narrow integer width (`int2`/`int4`),
    /// the PostgreSQL type name for the range error (e.g. `"smallint"`); `None`
    /// if it fits or this is not a width-narrowed integer. The single 64-bit
    /// integer storage is not itself range-enforced, so a write or cast into a
    /// narrowed column uses this to keep the advertised OID truthful.
    pub fn narrow_int_overflow(&self, value: i64) -> Option<&'static str> {
        match self {
            PgType::Int2 if i16::try_from(value).is_err() => Some("smallint"),
            PgType::Int4 if i32::try_from(value).is_err() => Some("integer"),
            PgType::Oid if u32::try_from(value).is_err() => Some("oid"),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for PgType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum SerializedPgType {
            Int2,
            Int4,
            Int8,
            Oid,
            Bool,
            Float4,
            Float8,
            Numeric { precision: Option<u32>, scale: u32 },
            Text,
            Varchar(Option<u32>),
            Bpchar(Option<u32>),
            Bytea,
            Uuid,
            Date,
            Time,
            Timestamp,
            Timestamptz,
            Interval,
            OidVector,
            Int2Vector,
            OidArray,
            Int2Array,
            Array(Box<SerializedPgType>),
        }

        fn convert<E: serde::de::Error>(value: SerializedPgType) -> Result<PgType, E> {
            let value = match value {
                SerializedPgType::Int2 => PgType::Int2,
                SerializedPgType::Int4 => PgType::Int4,
                SerializedPgType::Int8 => PgType::Int8,
                SerializedPgType::Oid => PgType::Oid,
                SerializedPgType::Bool => PgType::Bool,
                SerializedPgType::Float4 => PgType::Float4,
                SerializedPgType::Float8 => PgType::Float8,
                SerializedPgType::Numeric { precision, scale } => {
                    PgType::Numeric { precision, scale }
                }
                SerializedPgType::Text => PgType::Text,
                SerializedPgType::Varchar(length) => PgType::Varchar(length),
                SerializedPgType::Bpchar(length) => PgType::Bpchar(length),
                SerializedPgType::Bytea => PgType::Bytea,
                SerializedPgType::Uuid => PgType::Uuid,
                SerializedPgType::Date => PgType::Date,
                SerializedPgType::Time => PgType::Time,
                SerializedPgType::Timestamp => PgType::Timestamp,
                SerializedPgType::Timestamptz => PgType::Timestamptz,
                SerializedPgType::Interval => PgType::Interval,
                SerializedPgType::OidVector => PgType::OidVector,
                SerializedPgType::Int2Vector => PgType::Int2Vector,
                SerializedPgType::OidArray => PgType::OidArray,
                SerializedPgType::Int2Array => PgType::Int2Array,
                SerializedPgType::Array(element) => PgType::array(convert::<E>(*element)?)
                    .map_err(|error| E::custom(error.to_string()))?,
            };
            Ok(value)
        }

        convert(SerializedPgType::deserialize(deserializer)?)
    }
}

fn decode_length_typmod(typmod: i32) -> Option<u32> {
    let length = typmod.checked_sub(VARHDRSZ)?;
    u32::try_from(length).ok()
}

fn decode_numeric_typmod(typmod: i32) -> (Option<u32>, u32) {
    let Some(packed) = typmod.checked_sub(VARHDRSZ) else {
        return (None, 0);
    };
    let Ok(packed) = u32::try_from(packed) else {
        return (None, 0);
    };
    let precision = (packed >> 16) & 0xffff;
    if precision == 0 {
        return (None, 0);
    }
    let scale = packed & 0xffff;
    (Some(precision), scale)
}

/// The fallback wire type for a `DataType` with no declared label: the collapsed
/// families report their widest/most-general form (`Integer` → `int8`, `Text` →
/// `text`); every other type is 1:1. This reproduces the pre-`PgType` behavior
/// exactly, so any path lacking a declared label is unchanged.
impl From<&DataType> for PgType {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Integer => PgType::Int8,
            DataType::Text => PgType::Text,
            DataType::Boolean => PgType::Bool,
            DataType::Date => PgType::Date,
            DataType::Timestamp => PgType::Timestamp,
            DataType::Time => PgType::Time,
            DataType::TimestampTz => PgType::Timestamptz,
            DataType::Interval => PgType::Interval,
            DataType::Bytea => PgType::Bytea,
            DataType::Uuid => PgType::Uuid,
            DataType::Double => PgType::Float8,
            DataType::Real => PgType::Float4,
            DataType::Numeric { precision, scale } => PgType::Numeric {
                precision: *precision,
                scale: *scale,
            },
            DataType::Array(element) => PgType::array(PgType::from(element.as_ref()))
                .expect("DataType arrays always have supported scalar elements"),
        }
    }
}

impl PgType {
    pub fn array(element: PgType) -> crate::Result<Self> {
        if matches!(
            element,
            PgType::Array(_) | PgType::Int2Array | PgType::OidArray
        ) {
            return Err(crate::DbError::plan(
                crate::SqlState::DatatypeMismatch,
                "array elements cannot themselves be arrays",
            ));
        }
        if array_oid(&element).is_none() && !matches!(element, PgType::Int2 | PgType::Oid) {
            return Err(crate::DbError::plan(
                crate::SqlState::DatatypeMismatch,
                format!(
                    "{} cannot be used as an array element type",
                    element.format_type_name()
                ),
            ));
        }
        Ok(match element {
            PgType::Int2 => PgType::Int2Array,
            PgType::Oid => PgType::OidArray,
            other => PgType::Array(Box::new(other)),
        })
    }
}

fn array_oid(element: &PgType) -> Option<i32> {
    Some(match element {
        PgType::Bool => 1000,
        PgType::Bytea => 1001,
        PgType::Int2 => 1005,
        PgType::Int4 => 1007,
        PgType::Text => 1009,
        PgType::Bpchar(_) => 1014,
        PgType::Varchar(_) => 1015,
        PgType::Int8 => 1016,
        PgType::Float4 => 1021,
        PgType::Float8 => 1022,
        PgType::Oid => 1028,
        PgType::Timestamp => 1115,
        PgType::Date => 1182,
        PgType::Time => 1183,
        PgType::Timestamptz => 1185,
        PgType::Interval => 1187,
        PgType::Numeric { .. } => 1231,
        PgType::Uuid => 2951,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full wire mapping, checked against the canonical PostgreSQL
    /// `pg_type` OIDs and `typlen`s.
    #[test]
    fn oid_and_typlen_match_postgres() {
        let cases: &[(PgType, i32, i16)] = &[
            (PgType::Int2, 21, 2),
            (PgType::Int2Vector, 22, -1),
            (PgType::Int4, 23, 4),
            (PgType::Int8, 20, 8),
            (PgType::Oid, 26, 4),
            (PgType::OidVector, 30, -1),
            (PgType::Bool, 16, 1),
            (PgType::Float4, 700, 4),
            (PgType::Float8, 701, 8),
            (PgType::Int2Array, 1005, -1),
            (PgType::OidArray, 1028, -1),
            (
                PgType::Numeric {
                    precision: None,
                    scale: 0,
                },
                1700,
                -1,
            ),
            (PgType::Text, 25, -1),
            (PgType::Varchar(None), 1043, -1),
            (PgType::Varchar(Some(10)), 1043, -1),
            (PgType::Bpchar(None), 1042, -1),
            (PgType::Bpchar(Some(5)), 1042, -1),
            (PgType::Bytea, 17, -1),
            (PgType::Uuid, 2950, 16),
            (PgType::Date, 1082, 4),
            (PgType::Time, 1083, 8),
            (PgType::Timestamp, 1114, 8),
            (PgType::Timestamptz, 1184, 8),
            (PgType::Interval, 1186, 16),
        ];
        for (pg_type, oid, typlen) in cases {
            assert_eq!(pg_type.oid(), *oid, "oid for {pg_type:?}");
            assert_eq!(pg_type.typlen(), *typlen, "typlen for {pg_type:?}");
        }
    }

    #[test]
    fn oid_for_name_accepts_catalog_qualified_type_names() {
        assert_eq!(PgType::oid_for_name("int4"), Some(23));
        assert_eq!(PgType::oid_for_name("pg_catalog.int4"), Some(23));
        assert_eq!(PgType::oid_for_name("pg_catalog.oid"), Some(26));
        assert_eq!(PgType::oid_for_name("oidvector"), Some(30));
        assert_eq!(PgType::oid_for_name("_int2"), Some(1005));
        assert_eq!(PgType::oid_for_name("pg_catalog._oid"), Some(1028));
        assert_eq!(PgType::oid_for_name("PG_CATALOG.INTEGER"), Some(23));
        assert_eq!(PgType::oid_for_name("public.int4"), None);
    }

    #[test]
    fn supported_array_types_round_trip_between_name_oid_and_type() {
        let cases = [
            ("_bool", 1000),
            ("_bytea", 1001),
            ("_int2", 1005),
            ("_int4", 1007),
            ("_text", 1009),
            ("_bpchar", 1014),
            ("_varchar", 1015),
            ("_int8", 1016),
            ("_float4", 1021),
            ("_float8", 1022),
            ("_oid", 1028),
            ("_timestamp", 1115),
            ("_date", 1182),
            ("_time", 1183),
            ("_timestamptz", 1185),
            ("_interval", 1187),
            ("_numeric", 1231),
            ("_uuid", 2951),
        ];
        for (name, oid) in cases {
            assert_eq!(PgType::oid_for_name(name), Some(oid), "name {name}");
            let pg_type = PgType::from_oid_typmod(oid, -1).unwrap();
            assert_eq!(pg_type.oid(), i32::try_from(oid).unwrap(), "OID {oid}");
        }
    }

    #[test]
    fn array_constructor_rejects_non_scalar_wire_types() {
        assert!(PgType::array(PgType::OidVector).is_err());
        assert!(PgType::array(PgType::Int2Vector).is_err());
        assert!(PgType::array(PgType::Array(Box::new(PgType::Text))).is_err());
        assert_eq!(PgType::array(PgType::Int2).unwrap(), PgType::Int2Array);
        assert_eq!(PgType::array(PgType::Oid).unwrap(), PgType::OidArray);
        assert_eq!(PgType::array(PgType::Text).unwrap().oid(), 1009);
    }

    #[test]
    fn pg_type_deserialization_rejects_invalid_array_elements() {
        let text: PgType = serde_json::from_str(r#"{"Array":"Text"}"#).unwrap();
        assert_eq!(text, PgType::Array(Box::new(PgType::Text)));

        for json in [
            r#"{"Array":{"Array":"Text"}}"#,
            r#"{"Array":"OidVector"}"#,
            r#"{"Array":"Int2Vector"}"#,
        ] {
            assert!(serde_json::from_str::<PgType>(json).is_err(), "{json}");
        }
    }

    #[test]
    fn typmod_encodes_length_and_precision() {
        // Unconstrained types report -1.
        assert_eq!(PgType::Text.typmod(), -1);
        assert_eq!(PgType::Varchar(None).typmod(), -1);
        assert_eq!(PgType::Bpchar(None).typmod(), -1);
        assert_eq!(
            PgType::Numeric {
                precision: None,
                scale: 0
            }
            .typmod(),
            -1
        );
        assert_eq!(PgType::Int4.typmod(), -1);

        // varchar(n)/char(n) report n + VARHDRSZ.
        assert_eq!(PgType::Varchar(Some(10)).typmod(), 14);
        assert_eq!(PgType::Bpchar(Some(5)).typmod(), 9);

        // An absurd declared length/precision (a directly-constructed value) must
        // not panic or overflow; it degrades to "unconstrained". Precision and
        // scale are treated identically: either out of its 16-bit field yields -1.
        assert_eq!(PgType::Varchar(Some(u32::MAX)).typmod(), -1);
        assert_eq!(
            PgType::Numeric {
                precision: Some(u32::MAX),
                scale: u32::MAX
            }
            .typmod(),
            -1
        );
        assert_eq!(
            PgType::Numeric {
                precision: Some(10),
                scale: 70_000
            }
            .typmod(),
            -1
        );

        // numeric(p, s) packs precision in the high 16 bits, scale in the low 16,
        // plus VARHDRSZ — the inverse of how a client decodes it.
        let typmod = PgType::Numeric {
            precision: Some(10),
            scale: 2,
        }
        .typmod();
        assert_eq!(typmod, ((10 << 16) | 2) + 4);
        assert_eq!((typmod - 4) >> 16, 10);
        assert_eq!((typmod - 4) & 0xFFFF, 2);

        for pg_type in [
            PgType::Array(Box::new(PgType::Varchar(Some(10)))),
            PgType::Array(Box::new(PgType::Bpchar(Some(5)))),
            PgType::Array(Box::new(PgType::Numeric {
                precision: Some(10),
                scale: 2,
            })),
        ] {
            let decoded =
                PgType::from_oid_typmod(i64::from(pg_type.oid()), i64::from(pg_type.typmod()))
                    .unwrap();
            assert_eq!(decoded, pg_type);
        }
    }

    #[test]
    fn decodes_oid_typmod_for_catalog_functions() {
        assert_eq!(PgType::from_oid_typmod(23, -1), Some(PgType::Int4));
        assert_eq!(PgType::from_oid_typmod(26, -1), Some(PgType::Oid));
        assert_eq!(
            PgType::from_oid_typmod(1043, 14),
            Some(PgType::Varchar(Some(10)))
        );
        assert_eq!(
            PgType::from_oid_typmod(1700, ((12 << 16) | 2) + 4),
            Some(PgType::Numeric {
                precision: Some(12),
                scale: 2
            })
        );
        assert_eq!(PgType::from_oid_typmod(999_999, -1), None);
    }

    #[test]
    fn formats_type_names_for_catalog_functions() {
        assert_eq!(PgType::Int4.format_type_name(), "integer");
        assert_eq!(PgType::Oid.format_type_name(), "oid");
        assert_eq!(
            PgType::Varchar(Some(10)).format_type_name(),
            "character varying(10)"
        );
        assert_eq!(
            PgType::Numeric {
                precision: Some(12),
                scale: 2,
            }
            .format_type_name(),
            "numeric(12,2)"
        );
        assert_eq!(PgType::oid_for_name("double precision"), Some(701));
    }

    /// The label-free fallback reproduces the historical collapsed OIDs:
    /// integers as int8, character types as text.
    #[test]
    fn from_data_type_uses_collapsed_defaults() {
        assert_eq!(PgType::from(&DataType::Integer), PgType::Int8);
        assert_eq!(PgType::from(&DataType::Text), PgType::Text);
        assert_eq!(PgType::from(&DataType::Double), PgType::Float8);
        assert_eq!(PgType::from(&DataType::Real), PgType::Float4);
        assert_eq!(
            PgType::from(&DataType::Numeric {
                precision: Some(10),
                scale: 2
            }),
            PgType::Numeric {
                precision: Some(10),
                scale: 2
            }
        );
        assert_eq!(PgType::from(&DataType::TimestampTz), PgType::Timestamptz);
    }

    #[test]
    fn narrow_int_overflow_flags_out_of_range_values() {
        // int2 range is i16, int4 range is i32; int8 and non-integers never overflow.
        assert_eq!(PgType::Int2.narrow_int_overflow(32_767), None);
        assert_eq!(PgType::Int2.narrow_int_overflow(32_768), Some("smallint"));
        assert_eq!(PgType::Int2.narrow_int_overflow(-32_769), Some("smallint"));
        assert_eq!(PgType::Int4.narrow_int_overflow(2_147_483_647), None);
        assert_eq!(
            PgType::Int4.narrow_int_overflow(2_147_483_648),
            Some("integer")
        );
        assert_eq!(PgType::Int8.narrow_int_overflow(i64::MAX), None);
        assert_eq!(PgType::Oid.narrow_int_overflow(4_294_967_295), None);
        assert_eq!(PgType::Oid.narrow_int_overflow(-1), Some("oid"));
        assert_eq!(PgType::Oid.narrow_int_overflow(4_294_967_296), Some("oid"));
        assert_eq!(PgType::Text.narrow_int_overflow(i64::MAX), None);
    }

    #[test]
    fn data_type_collapses_widths_and_char_kinds() {
        // The refined wire types collapse back to the storage type.
        for pg_type in [PgType::Int2, PgType::Int4, PgType::Int8, PgType::Oid] {
            assert_eq!(pg_type.data_type(), DataType::Integer);
        }
        for pg_type in [
            PgType::Text,
            PgType::Varchar(Some(10)),
            PgType::Bpchar(None),
            PgType::OidVector,
            PgType::Int2Vector,
            PgType::OidArray,
            PgType::Int2Array,
        ] {
            assert_eq!(pg_type.data_type(), DataType::Text);
        }
        assert_eq!(PgType::Float4.data_type(), DataType::Real);
        assert_eq!(PgType::Float8.data_type(), DataType::Double);

        // `data_type()` is a left inverse of the label-free `From` for every type.
        let all = [
            DataType::Integer,
            DataType::Text,
            DataType::Boolean,
            DataType::Date,
            DataType::Timestamp,
            DataType::Time,
            DataType::TimestampTz,
            DataType::Interval,
            DataType::Bytea,
            DataType::Uuid,
            DataType::Double,
            DataType::Real,
            DataType::Numeric {
                precision: Some(10),
                scale: 2,
            },
            DataType::Array(Box::new(DataType::Text)),
        ];
        for data_type in all {
            assert_eq!(PgType::from(&data_type).data_type(), data_type);
        }
    }
}
