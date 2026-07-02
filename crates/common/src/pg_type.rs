use serde::{Deserialize, Serialize};

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgType {
    Int2,
    Int4,
    Int8,
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
}

impl PgType {
    /// The PostgreSQL type OID reported on the wire.
    pub fn oid(&self) -> i32 {
        match self {
            PgType::Int2 => 21,
            PgType::Int4 => 23,
            PgType::Int8 => 20,
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
        }
    }

    /// The fixed on-wire byte length (`typlen`), or `-1` for a variable-length type.
    pub fn typlen(&self) -> i16 {
        match self {
            PgType::Bool => 1,
            PgType::Int2 => 2,
            PgType::Int4 | PgType::Float4 | PgType::Date => 4,
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
            | PgType::Bytea => -1,
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
            _ => return -1,
        };
        i32::try_from(modifier).unwrap_or(-1)
    }
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
        }
    }
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
            (PgType::Int4, 23, 4),
            (PgType::Int8, 20, 8),
            (PgType::Bool, 16, 1),
            (PgType::Float4, 700, 4),
            (PgType::Float8, 701, 8),
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
}
