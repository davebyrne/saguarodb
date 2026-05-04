use common::{DbError, Result};

use crate::CatalogSnapshot;

pub fn serialize_catalog(snapshot: &CatalogSnapshot) -> Result<Vec<u8>> {
    serde_json::to_vec(snapshot)
        .map_err(|err| DbError::internal(format!("failed to serialize catalog: {err}")))
}

pub fn deserialize_catalog(bytes: &[u8]) -> Result<CatalogSnapshot> {
    serde_json::from_slice(bytes)
        .map_err(|err| DbError::internal(format!("failed to deserialize catalog: {err}")))
}
