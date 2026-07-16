//! Shared allocation limits for collections nested in durable catalog values.

#![cfg_attr(
    not(test),
    deny(
        clippy::arithmetic_side_effects,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::indexing_slicing
    )
)]

use std::{collections::BTreeMap, fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer,
    de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
};

/// Maximum number of entries in one nested durable catalog collection.
pub(crate) const MAX_DURABLE_COLLECTION_ITEMS: usize = 65_536;
const MAX_DURABLE_BYTE_ITEMS: usize = 64 * 1024 * 1024;

pub(crate) fn deserialize_bounded_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserialize_bounded_vec_with_limit::<D, T, MAX_DURABLE_COLLECTION_ITEMS>(deserializer)
}

pub(crate) fn deserialize_bounded_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec_named(deserializer, MAX_DURABLE_BYTE_ITEMS, "durable byte vector")
}

pub(crate) fn deserialize_bounded_vec_with_limit<'de, D, T, const LIMIT: usize>(
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserialize_bounded_vec_named(deserializer, LIMIT, "durable collection")
}

/// Deserializes a durable vector with a caller-defined limit and diagnostic name.
///
/// This is exported for durable decoders in other workspace crates so all such
/// vectors use the same fallible reservation implementation.
pub fn deserialize_bounded_vec_named<'de, D, T>(
    deserializer: D,
    limit: usize,
    description: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T> {
        limit: usize,
        description: &'static str,
        marker: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "{} with at most {} items",
                self.description, self.limit
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let hint = sequence.size_hint().unwrap_or(0);
            if hint > self.limit {
                return Err(A::Error::custom(format!(
                    "{} exceeds {} items",
                    self.description, self.limit
                )));
            }
            let mut values = Vec::new();
            values
                .try_reserve(hint)
                .map_err(|_| A::Error::custom("cannot allocate durable collection"))?;
            while values.len() < self.limit {
                let Some(value) = sequence.next_element()? else {
                    return Ok(values);
                };
                if values.len() == values.capacity() {
                    values
                        .try_reserve(1)
                        .map_err(|_| A::Error::custom("cannot grow durable collection"))?;
                }
                values.push(value);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format!(
                    "{} exceeds {} items",
                    self.description, self.limit
                )));
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor {
        limit,
        description,
        marker: PhantomData,
    })
}

pub(crate) fn deserialize_bounded_map<'de, D, K, V>(
    deserializer: D,
) -> Result<BTreeMap<K, V>, D::Error>
where
    D: Deserializer<'de>,
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    struct BoundedMapVisitor<K, V>(PhantomData<(K, V)>);

    impl<'de, K, V> Visitor<'de> for BoundedMapVisitor<K, V>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
    {
        type Value = BTreeMap<K, V>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a durable map of at most {MAX_DURABLE_COLLECTION_ITEMS} entries"
            )
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            if map
                .size_hint()
                .is_some_and(|hint| hint > MAX_DURABLE_COLLECTION_ITEMS)
            {
                return Err(A::Error::custom("durable map exceeds item limit"));
            }
            let mut values = BTreeMap::new();
            while values.len() < MAX_DURABLE_COLLECTION_ITEMS {
                let Some((key, value)) = map.next_entry()? else {
                    return Ok(values);
                };
                if values.insert(key, value).is_some() {
                    return Err(A::Error::custom("durable map repeats a key"));
                }
            }
            if map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {
                return Err(A::Error::custom("durable map exceeds item limit"));
            }
            Ok(values)
        }
    }

    deserializer.deserialize_map(BoundedMapVisitor(PhantomData))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{MAX_DURABLE_COLLECTION_ITEMS, deserialize_bounded_vec};

    #[derive(Debug, Deserialize)]
    struct DurableList {
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        values: Vec<u8>,
    }

    #[test]
    fn nested_durable_collection_rejects_an_extra_item() {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "values": vec![0_u8; MAX_DURABLE_COLLECTION_ITEMS + 1]
        }))
        .unwrap();

        let error = serde_json::from_slice::<DurableList>(&bytes).unwrap_err();
        assert!(error.to_string().contains("exceeds 65536 items"));
    }

    #[test]
    fn nested_durable_collection_accepts_its_limit() {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "values": vec![0_u8; MAX_DURABLE_COLLECTION_ITEMS]
        }))
        .unwrap();

        let decoded: DurableList = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.values.len(), MAX_DURABLE_COLLECTION_ITEMS);
    }
}
