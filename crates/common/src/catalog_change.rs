//! Durable, object-typed catalog mutations shared by live DDL and recovery.

use std::collections::BTreeMap;

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
};

use crate::{
    ColumnObjectId, ConstraintId, FileId, IndexId, IndexSchema, NamespaceSchema, SchemaId,
    SequenceId, SequenceSchema, TableId, TableSchema, TableStatistics, ViewSchema,
};

pub const CATALOG_CHANGE_SET_VERSION: u16 = 1;
pub const MAX_CATALOG_CHANGE_MUTATIONS: usize = 65_536;

/// Stable address of an independently replaceable catalog object.
///
/// Columns remain nested in relation objects; the column form exists so the
/// dependency graph can address their durable identities without a second map.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CatalogObjectId {
    Schema(SchemaId),
    Table(TableId),
    View(TableId),
    Index(IndexId),
    Sequence(SequenceId),
    Constraint(ConstraintId),
    Statistics(TableId),
    Column {
        relation: TableId,
        column: ColumnObjectId,
    },
}

/// Complete durable value of an independently replaceable catalog object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogObject {
    Schema(NamespaceSchema),
    Table(TableSchema),
    View(ViewSchema),
    Index(IndexSchema),
    Sequence(SequenceSchema),
    /// Reserved until first-class constraint schemas are introduced.
    Constraint(ConstraintId),
    Statistics {
        table: TableId,
        statistics: TableStatistics,
    },
}

impl CatalogObject {
    pub fn id(&self) -> CatalogObjectId {
        match self {
            Self::Schema(schema) => CatalogObjectId::Schema(schema.id),
            Self::Table(schema) => CatalogObjectId::Table(schema.id),
            Self::View(schema) => CatalogObjectId::View(schema.id),
            Self::Index(schema) => CatalogObjectId::Index(schema.id),
            Self::Sequence(schema) => CatalogObjectId::Sequence(schema.id),
            Self::Constraint(id) => CatalogObjectId::Constraint(*id),
            Self::Statistics { table, .. } => CatalogObjectId::Statistics(*table),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogMutation {
    pub before: Option<CatalogObject>,
    pub after: Option<CatalogObject>,
}

impl CatalogMutation {
    pub fn id(&self) -> Option<CatalogObjectId> {
        self.after
            .as_ref()
            .or(self.before.as_ref())
            .map(CatalogObject::id)
    }
}

/// Allocator reservation carried even by catalog changes whose transaction
/// later aborts. Per-relation column high-water values preserve stable column
/// identities without making columns top-level objects.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogAllocatorHighWater {
    pub next_schema_id: SchemaId,
    pub next_table_id: TableId,
    pub next_index_id: IndexId,
    pub next_sequence_id: SequenceId,
    pub next_dictionary_id: u32,
    pub next_storage_id: FileId,
    pub next_constraint_id: ConstraintId,
    #[serde(deserialize_with = "deserialize_column_high_water")]
    pub next_column_object_ids: BTreeMap<TableId, ColumnObjectId>,
    #[serde(deserialize_with = "deserialize_foreign_key_high_water")]
    pub next_foreign_key_ids: BTreeMap<TableId, u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogChangeSet {
    pub version: u16,
    #[serde(deserialize_with = "deserialize_mutations")]
    pub mutations: Vec<CatalogMutation>,
    pub allocator_high_water: CatalogAllocatorHighWater,
}

fn deserialize_mutations<'de, D>(deserializer: D) -> Result<Vec<CatalogMutation>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MutationsVisitor;

    impl<'de> Visitor<'de> for MutationsVisitor {
        type Value = Vec<CatalogMutation>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded catalog mutation list")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let hint = sequence.size_hint().unwrap_or(0);
            if hint > MAX_CATALOG_CHANGE_MUTATIONS {
                return Err(A::Error::custom(
                    "catalog change set has too many mutations",
                ));
            }
            let mut mutations = Vec::new();
            mutations
                .try_reserve(hint)
                .map_err(|_| A::Error::custom("cannot allocate catalog mutation list"))?;
            while mutations.len() < MAX_CATALOG_CHANGE_MUTATIONS {
                let Some(mutation) = sequence.next_element()? else {
                    return Ok(mutations);
                };
                mutations.push(mutation);
            }
            // Probe for an over-limit element without materializing its nested
            // strings and vectors. `IgnoredAny` consumes the representation
            // without allocating a `CatalogMutation`.
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(
                    "catalog change set has too many mutations",
                ));
            }
            Ok(mutations)
        }
    }

    deserializer.deserialize_seq(MutationsVisitor)
}

fn deserialize_column_high_water<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<TableId, ColumnObjectId>, D::Error>
where
    D: Deserializer<'de>,
{
    struct HighWaterVisitor;

    impl<'de> Visitor<'de> for HighWaterVisitor {
        type Value = BTreeMap<TableId, ColumnObjectId>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded stable-column allocator map")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            if map
                .size_hint()
                .is_some_and(|hint| hint > MAX_CATALOG_CHANGE_MUTATIONS)
            {
                return Err(A::Error::custom(
                    "catalog change set has too many column allocators",
                ));
            }
            let mut high_water = BTreeMap::new();
            while let Some((relation, column)) = map.next_entry()? {
                if high_water.len() == MAX_CATALOG_CHANGE_MUTATIONS {
                    return Err(A::Error::custom(
                        "catalog change set has too many column allocators",
                    ));
                }
                if high_water.insert(relation, column).is_some() {
                    return Err(A::Error::custom(
                        "catalog change set repeats a column allocator",
                    ));
                }
            }
            Ok(high_water)
        }
    }

    deserializer.deserialize_map(HighWaterVisitor)
}

fn deserialize_foreign_key_high_water<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<TableId, u32>, D::Error>
where
    D: Deserializer<'de>,
{
    struct HighWaterVisitor;

    impl<'de> Visitor<'de> for HighWaterVisitor {
        type Value = BTreeMap<TableId, u32>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded foreign-key allocator map")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            if map
                .size_hint()
                .is_some_and(|hint| hint > MAX_CATALOG_CHANGE_MUTATIONS)
            {
                return Err(A::Error::custom(
                    "catalog change set has too many foreign-key allocators",
                ));
            }
            let mut high_water = BTreeMap::new();
            while let Some((relation, next_foreign_key)) = map.next_entry()? {
                if high_water.len() == MAX_CATALOG_CHANGE_MUTATIONS {
                    return Err(A::Error::custom(
                        "catalog change set has too many foreign-key allocators",
                    ));
                }
                if high_water.insert(relation, next_foreign_key).is_some() {
                    return Err(A::Error::custom(
                        "catalog change set repeats a foreign-key allocator",
                    ));
                }
            }
            Ok(high_water)
        }
    }

    deserializer.deserialize_map(HighWaterVisitor)
}

impl CatalogChangeSet {
    /// Builds a deterministic object-id-sorted change set from complete object maps.
    pub fn between(
        before: &BTreeMap<CatalogObjectId, CatalogObject>,
        after: &BTreeMap<CatalogObjectId, CatalogObject>,
        allocator_high_water: CatalogAllocatorHighWater,
    ) -> Self {
        let mut ids = before
            .keys()
            .chain(after.keys())
            .copied()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        let mutations = ids
            .into_iter()
            .filter_map(|id| {
                let old = before.get(&id);
                let new = after.get(&id);
                (old != new).then(|| CatalogMutation {
                    before: old.cloned(),
                    after: new.cloned(),
                })
            })
            .collect();
        Self {
            version: CATALOG_CHANGE_SET_VERSION,
            mutations,
            allocator_high_water,
        }
    }

    pub fn validate_shape(&self) -> std::result::Result<(), &'static str> {
        if self.version != CATALOG_CHANGE_SET_VERSION {
            return Err("unsupported catalog change-set version");
        }
        if self.mutations.len() > MAX_CATALOG_CHANGE_MUTATIONS {
            return Err("catalog change set has too many mutations");
        }
        if self.allocator_high_water.next_column_object_ids.len() > MAX_CATALOG_CHANGE_MUTATIONS {
            return Err("catalog change set has too many column allocators");
        }
        if self.allocator_high_water.next_foreign_key_ids.len() > MAX_CATALOG_CHANGE_MUTATIONS {
            return Err("catalog change set has too many foreign-key allocators");
        }
        let mut previous = None;
        for mutation in &self.mutations {
            let Some(id) = mutation.id() else {
                return Err("catalog mutation has neither a before nor an after object");
            };
            if mutation
                .before
                .as_ref()
                .is_some_and(|object| object.id() != id)
                || mutation
                    .after
                    .as_ref()
                    .is_some_and(|object| object.id() != id)
            {
                return Err("catalog mutation before/after object ids differ");
            }
            if mutation.before == mutation.after {
                return Err("catalog mutation does not change its object");
            }
            if mutation
                .before
                .iter()
                .chain(mutation.after.iter())
                .any(|object| matches!(object, CatalogObject::Statistics { statistics, .. } if !statistics.is_finite()))
            {
                return Err("catalog change contains non-finite statistics");
            }
            if mutation
                .before
                .iter()
                .chain(mutation.after.iter())
                .any(|object| matches!(object, CatalogObject::Constraint(_)))
            {
                return Err("catalog constraint objects are not supported by this format version");
            }
            if previous.is_some_and(|candidate| candidate >= id) {
                return Err("catalog mutations are not strictly object-id sorted");
            }
            previous = Some(id);
        }
        Ok(())
    }
}
