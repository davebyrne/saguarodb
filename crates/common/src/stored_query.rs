//! Durable, catalog-resolved query representation for view definitions.

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeSeed, Error as _, SeqAccess, Visitor},
};
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
};

use crate::{BindingId, ColumnObjectId, DataType, FunctionId, PgType, SequenceId, TableId, Value};

pub const STORED_QUERY_VERSION: u32 = 1;
pub const MAX_STORED_QUERY_NODES: usize = 262_144;
pub const MAX_STORED_QUERY_LIST_ITEMS: usize = 16_384;
pub const MAX_STORED_QUERY_COLUMNS: usize = 65_536;
pub const MAX_STORED_QUERY_DECODE_ITEMS: usize = 1_048_576;
pub const MAX_STORED_QUERY_DEPTH: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StoredQueryV1 {
    pub version: u32,
    pub body: StoredQueryBody,
    pub order_by: Vec<StoredOrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub row_lock: Option<StoredRowLock>,
    pub correlations: Vec<StoredCorrelatedColumn>,
}

#[derive(Deserialize)]
struct StoredQueryV1Repr {
    version: u32,
    body: StoredQueryBody,
    #[serde(deserialize_with = "deserialize_bounded_vec")]
    order_by: Vec<StoredOrderBy>,
    limit: Option<u64>,
    offset: Option<u64>,
    row_lock: Option<StoredRowLock>,
    #[serde(deserialize_with = "deserialize_bounded_vec")]
    correlations: Vec<StoredCorrelatedColumn>,
}

thread_local! {
    static STORED_QUERY_DECODE_BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
}

struct DecodeBudgetGuard;

impl Drop for DecodeBudgetGuard {
    fn drop(&mut self) {
        STORED_QUERY_DECODE_BUDGET.with(|budget| budget.set(None));
    }
}

fn with_new_decode_budget<T>(limit: usize, decode: impl FnOnce() -> T) -> T {
    STORED_QUERY_DECODE_BUDGET.with(|budget| budget.set(Some(limit)));
    let _guard = DecodeBudgetGuard;
    decode()
}

fn consume_decode_budget() -> bool {
    consume_decode_budget_units(1)
}

fn consume_decode_budget_units(units: usize) -> bool {
    STORED_QUERY_DECODE_BUDGET.with(|budget| match budget.get() {
        Some(remaining) => remaining.checked_sub(units).is_some_and(|next| {
            budget.set(Some(next));
            true
        }),
        None => true,
    })
}

struct DecodeBudgetSeed<T, const UNITS: usize = 1>(PhantomData<T>);

impl<'de, T, const UNITS: usize> DeserializeSeed<'de> for DecodeBudgetSeed<T, UNITS>
where
    T: Deserialize<'de>,
{
    type Value = T;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if !consume_decode_budget_units(UNITS) {
            return Err(D::Error::custom("stored query exceeds decode node budget"));
        }
        T::deserialize(deserializer)
    }
}

fn deserialize_budgeted_box<'de, D, T>(deserializer: D) -> Result<Box<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    if !consume_decode_budget() {
        return Err(D::Error::custom("stored query exceeds decode node budget"));
    }
    Box::<T>::deserialize(deserializer)
}

fn deserialize_budgeted_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BudgetedOptionVisitor<T>(PhantomData<T>);
    impl<'de, T> Visitor<'de> for BudgetedOptionVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Option<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an optional stored-query node")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            if !consume_decode_budget() {
                return Err(D::Error::custom("stored query exceeds decode node budget"));
            }
            T::deserialize(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(BudgetedOptionVisitor(PhantomData))
}

impl<'de> Deserialize<'de> for StoredQueryV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decode = || {
            if !consume_decode_budget() {
                return Err(D::Error::custom("stored query exceeds decode node budget"));
            }
            let decoded = StoredQueryV1Repr::deserialize(deserializer)?;
            Ok(Self {
                version: decoded.version,
                body: decoded.body,
                order_by: decoded.order_by,
                limit: decoded.limit,
                offset: decoded.offset,
                row_lock: decoded.row_lock,
                correlations: decoded.correlations,
            })
        };
        let active = STORED_QUERY_DECODE_BUDGET.with(|budget| budget.get().is_some());
        if active {
            decode()
        } else {
            with_new_decode_budget(MAX_STORED_QUERY_DECODE_ITEMS, decode)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRowLock {
    pub table: TableId,
    pub mode: StoredTupleLockMode,
    pub wait_policy: StoredTupleLockWaitPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredTupleLockMode {
    KeyShare,
    Share,
    NoKeyUpdate,
    Update,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredTupleLockWaitPolicy {
    Block,
    NoWait,
    SkipLocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCorrelatedColumn {
    pub outer: StoredQueryExpr,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredQueryBody {
    Select(Box<StoredSelect>),
    Values(StoredValues),
    SetOp(StoredSetOp),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredValues {
    #[serde(deserialize_with = "deserialize_bounded_rows")]
    pub rows: Vec<Vec<StoredQueryExpr>>,
    #[serde(deserialize_with = "deserialize_bounded_columns")]
    pub output_schema: Vec<StoredQueryColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSetOp {
    pub op: StoredSetOperator,
    pub all: bool,
    pub left: Box<StoredQueryV1>,
    pub right: Box<StoredQueryV1>,
    #[serde(deserialize_with = "deserialize_bounded_columns")]
    pub output_schema: Vec<StoredQueryColumn>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredSetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSelect {
    pub distinct: Option<StoredDistinct>,
    #[serde(deserialize_with = "deserialize_bounded_columns")]
    pub columns: Vec<StoredSelectItem>,
    #[serde(deserialize_with = "deserialize_budgeted_option")]
    pub from: Option<StoredFrom>,
    #[serde(deserialize_with = "deserialize_budgeted_option")]
    pub filter: Option<StoredQueryExpr>,
    #[serde(deserialize_with = "deserialize_bounded_vec")]
    pub group_by: Vec<StoredQueryExpr>,
    #[serde(deserialize_with = "deserialize_budgeted_option")]
    pub having: Option<StoredQueryExpr>,
    #[serde(deserialize_with = "deserialize_bounded_columns")]
    pub output_schema: Vec<StoredQueryColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredDistinct {
    All,
    On(#[serde(deserialize_with = "deserialize_bounded_vec")] Vec<StoredQueryExpr>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSelectItem {
    pub expr: StoredQueryExpr,
    pub alias: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredFrom {
    Table {
        table: TableId,
        range: BindingId,
        alias: Option<String>,
    },
    System {
        relation_oid: i64,
        range: BindingId,
        alias: Option<String>,
        #[serde(deserialize_with = "deserialize_bounded_columns")]
        schema: Vec<StoredRangeColumn>,
    },
    Derived {
        query: Box<StoredQueryV1>,
        range: BindingId,
        alias: String,
        #[serde(deserialize_with = "deserialize_bounded_columns")]
        schema: Vec<StoredRangeColumn>,
        lateral: bool,
    },
    TableFunction {
        function: FunctionId,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        args: Vec<StoredQueryExpr>,
        range: BindingId,
        alias: String,
        #[serde(deserialize_with = "deserialize_bounded_columns")]
        schema: Vec<StoredRangeColumn>,
    },
    Join {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        left: Box<Self>,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        right: Box<Self>,
        join_type: StoredJoinType,
        #[serde(deserialize_with = "deserialize_budgeted_option")]
        condition: Option<StoredQueryExpr>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredJoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRangeColumn {
    pub name: String,
    pub data_type: DataType,
    pub pg_type: PgType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredQueryColumn {
    pub name: String,
    pub data_type: DataType,
    pub pg_type: PgType,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredOrderBy {
    pub expr: StoredQueryExpr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredColumnReference {
    Catalog(ColumnObjectId),
    Position(u32),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredQueryExpr {
    Literal {
        value: Value,
        data_type: DataType,
        nullable: bool,
    },
    InputRef {
        range: BindingId,
        column: StoredColumnReference,
        data_type: DataType,
        nullable: bool,
    },
    Binary {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        left: Box<Self>,
        op: StoredQueryBinOp,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        right: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Unary {
        op: StoredQueryUnaryOp,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Function {
        function: FunctionId,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        args: Vec<Self>,
        data_type: DataType,
        pg_type: Option<PgType>,
        nullable: bool,
    },
    Array {
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        elements: Vec<Self>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        dimensions: Vec<u32>,
        element_type: DataType,
        data_type: DataType,
        nullable: bool,
    },
    ArraySubscript {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        array: Box<Self>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        subscripts: Vec<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Any {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        left: Box<Self>,
        op: StoredQueryBinOp,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        array: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Nextval {
        sequence: SequenceId,
        data_type: DataType,
        nullable: bool,
    },
    Currval {
        sequence: SequenceId,
        data_type: DataType,
        nullable: bool,
    },
    Setval {
        sequence: SequenceId,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        value: Box<Self>,
        #[serde(deserialize_with = "deserialize_budgeted_option")]
        is_called: Option<Box<Self>>,
        data_type: DataType,
        nullable: bool,
    },
    Aggregate {
        function: FunctionId,
        #[serde(deserialize_with = "deserialize_budgeted_option")]
        arg: Option<Box<Self>>,
        distinct: bool,
        data_type: DataType,
        nullable: bool,
    },
    LocalRef {
        output: u32,
        data_type: DataType,
        nullable: bool,
    },
    OuterRef {
        correlation: u32,
        data_type: DataType,
        nullable: bool,
    },
    IsNull {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    IsNotNull {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    InList {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        list: Vec<Self>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Between {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        low: Box<Self>,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        high: Box<Self>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Like {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        pattern: Box<Self>,
        negated: bool,
        case_insensitive: bool,
        escape: Option<char>,
        data_type: DataType,
        nullable: bool,
    },
    Case {
        #[serde(deserialize_with = "deserialize_budgeted_option")]
        operand: Option<Box<Self>>,
        #[serde(deserialize_with = "deserialize_bounded_when_clauses")]
        when_clauses: Vec<(Self, Self)>,
        #[serde(deserialize_with = "deserialize_budgeted_option")]
        else_clause: Option<Box<Self>>,
        flow_sensitive_nullable: bool,
        data_type: DataType,
        nullable: bool,
    },
    Cast {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        data_type: DataType,
        pg_type: PgType,
        nullable: bool,
    },
    ScalarSubquery {
        query: Box<StoredQueryV1>,
        data_type: DataType,
        nullable: bool,
    },
    Exists {
        query: Box<StoredQueryV1>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    InSubquery {
        #[serde(deserialize_with = "deserialize_budgeted_box")]
        expr: Box<Self>,
        query: Box<StoredQueryV1>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredQueryBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredQueryUnaryOp {
    Neg,
    Not,
}

pub const UNNEST_FUNCTION_ID: FunctionId = 2_331;
pub const GENERATE_SERIES_FUNCTION_ID: FunctionId = 1_067;

impl StoredQueryExpr {
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Literal { data_type, .. }
            | Self::InputRef { data_type, .. }
            | Self::Binary { data_type, .. }
            | Self::Unary { data_type, .. }
            | Self::Function { data_type, .. }
            | Self::Array { data_type, .. }
            | Self::ArraySubscript { data_type, .. }
            | Self::Any { data_type, .. }
            | Self::Nextval { data_type, .. }
            | Self::Currval { data_type, .. }
            | Self::Setval { data_type, .. }
            | Self::Aggregate { data_type, .. }
            | Self::LocalRef { data_type, .. }
            | Self::OuterRef { data_type, .. }
            | Self::IsNull { data_type, .. }
            | Self::IsNotNull { data_type, .. }
            | Self::InList { data_type, .. }
            | Self::Between { data_type, .. }
            | Self::Like { data_type, .. }
            | Self::Case { data_type, .. }
            | Self::Cast { data_type, .. }
            | Self::ScalarSubquery { data_type, .. }
            | Self::Exists { data_type, .. }
            | Self::InSubquery { data_type, .. } => data_type,
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            Self::Literal { nullable, .. }
            | Self::InputRef { nullable, .. }
            | Self::Binary { nullable, .. }
            | Self::Unary { nullable, .. }
            | Self::Function { nullable, .. }
            | Self::Array { nullable, .. }
            | Self::ArraySubscript { nullable, .. }
            | Self::Any { nullable, .. }
            | Self::Nextval { nullable, .. }
            | Self::Currval { nullable, .. }
            | Self::Setval { nullable, .. }
            | Self::Aggregate { nullable, .. }
            | Self::LocalRef { nullable, .. }
            | Self::OuterRef { nullable, .. }
            | Self::IsNull { nullable, .. }
            | Self::IsNotNull { nullable, .. }
            | Self::InList { nullable, .. }
            | Self::Between { nullable, .. }
            | Self::Like { nullable, .. }
            | Self::Case { nullable, .. }
            | Self::Cast { nullable, .. }
            | Self::ScalarSubquery { nullable, .. }
            | Self::Exists { nullable, .. }
            | Self::InSubquery { nullable, .. } => *nullable,
        }
    }
}

fn deserialize_bounded_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserialize_bounded_vec_with_limit::<D, T, MAX_STORED_QUERY_LIST_ITEMS, 1>(deserializer)
}

fn deserialize_bounded_columns<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserialize_bounded_vec_with_limit::<D, T, MAX_STORED_QUERY_COLUMNS, 1>(deserializer)
}

fn deserialize_bounded_when_clauses<'de, D>(
    deserializer: D,
) -> Result<Vec<(StoredQueryExpr, StoredQueryExpr)>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec_with_limit::<
        D,
        (StoredQueryExpr, StoredQueryExpr),
        MAX_STORED_QUERY_LIST_ITEMS,
        2,
    >(deserializer)
}

fn deserialize_bounded_vec_with_limit<'de, D, T, const LIMIT: usize, const UNITS: usize>(
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T, const LIMIT: usize, const UNITS: usize>(PhantomData<T>);
    impl<'de, T, const LIMIT: usize, const UNITS: usize> Visitor<'de>
        for BoundedVecVisitor<T, LIMIT, UNITS>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a stored-query list of at most {LIMIT} items")
        }
        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence.size_hint().is_some_and(|size| size > LIMIT) {
                return Err(A::Error::custom("stored query list exceeds limit"));
            }
            let mut values = Vec::new();
            while let Some(value) =
                sequence.next_element_seed(DecodeBudgetSeed::<T, UNITS>(PhantomData))?
            {
                if values.len() >= LIMIT {
                    return Err(A::Error::custom("stored query list exceeds limit"));
                }
                values
                    .try_reserve(1)
                    .map_err(|_| A::Error::custom("stored query list allocation failed"))?;
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(BoundedVecVisitor::<T, LIMIT, UNITS>(PhantomData))
}

fn deserialize_bounded_rows<'de, D>(deserializer: D) -> Result<Vec<Vec<StoredQueryExpr>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoundedRowsVisitor;
    impl<'de> Visitor<'de> for BoundedRowsVisitor {
        type Value = Vec<Vec<StoredQueryExpr>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a stored VALUES row list with bounded rows and columns"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|size| size > MAX_STORED_QUERY_LIST_ITEMS)
            {
                return Err(A::Error::custom("stored query list exceeds limit"));
            }
            let mut rows = Vec::new();
            while let Some(row) = sequence
                .next_element_seed(DecodeBudgetSeed::<BoundedStoredQueryRow>(PhantomData))?
            {
                if rows.len() >= MAX_STORED_QUERY_LIST_ITEMS {
                    return Err(A::Error::custom("stored query list exceeds limit"));
                }
                rows.try_reserve(1)
                    .map_err(|_| A::Error::custom("stored query list allocation failed"))?;
                rows.push(row.0);
            }
            Ok(rows)
        }
    }

    deserializer.deserialize_seq(BoundedRowsVisitor)
}

struct BoundedStoredQueryRow(Vec<StoredQueryExpr>);

impl<'de> Deserialize<'de> for BoundedStoredQueryRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_bounded_columns(deserializer).map(Self)
    }
}

pub fn stored_query_function_exists(id: FunctionId) -> bool {
    crate::lookup_scalar_function_by_id(id).is_some()
        || matches!(
            id,
            UNNEST_FUNCTION_ID
                | GENERATE_SERIES_FUNCTION_ID
                | 2_147
                | 2_107
                | 2_100
                | 2_131
                | 2_115
                | 2_713
                | 2_712
                | 2_644
                | 2_643
                | 2_517
                | 2_518
                | 2_335
                | 3_538
        )
}

fn stored_aggregate_function_exists(id: FunctionId) -> bool {
    matches!(
        id,
        2_147
            | 2_107
            | 2_100
            | 2_131
            | 2_115
            | 2_713
            | 2_712
            | 2_644
            | 2_643
            | 2_517
            | 2_518
            | 2_335
            | 3_538
    )
}

impl StoredQueryV1 {
    pub fn output_schema(&self) -> &[StoredQueryColumn] {
        match &self.body {
            StoredQueryBody::Select(select) => &select.output_schema,
            StoredQueryBody::Values(values) => &values.output_schema,
            StoredQueryBody::SetOp(set_op) => &set_op.output_schema,
        }
    }

    /// Validates structural limits and returns the exact catalog identities used
    /// by the resolved query. Existence and type checks belong to the catalog.
    pub fn referenced_catalog_objects(&self) -> crate::Result<BTreeSet<crate::CatalogObjectId>> {
        let mut objects = BTreeSet::new();
        let mut nodes = 0;
        visit_query(self, None, 0, &mut nodes, &mut objects)?;
        Ok(objects)
    }

    pub fn for_each_catalog_column_reference(
        &self,
        visitor: &mut impl FnMut(TableId, ColumnObjectId, &DataType, bool, bool),
    ) -> crate::Result<()> {
        collect_query_references(self, None, visitor, &mut |_, _| {})
    }

    pub fn output_nullability(&self) -> crate::Result<Vec<bool>> {
        match &self.body {
            StoredQueryBody::Select(select) => Ok(select
                .columns
                .iter()
                .map(|item| item.expr.nullable())
                .collect()),
            StoredQueryBody::Values(values) => {
                let mut nullable = vec![false; values.output_schema.len()];
                for row in &values.rows {
                    if row.len() != nullable.len() {
                        return Err(crate::DbError::internal(
                            "stored VALUES row width does not match output schema",
                        ));
                    }
                    for (output, expr) in nullable.iter_mut().zip(row) {
                        *output |= expr.nullable();
                    }
                }
                Ok(nullable)
            }
            StoredQueryBody::SetOp(set_op) => {
                let left = set_op.left.output_nullability()?;
                let right = set_op.right.output_nullability()?;
                if left.len() != right.len() || left.len() != set_op.output_schema.len() {
                    return Err(crate::DbError::internal(
                        "stored set operation output width mismatch",
                    ));
                }
                Ok(left
                    .into_iter()
                    .zip(right)
                    .map(|(left, right)| left || right)
                    .collect())
            }
        }
    }

    /// Validates every query output's exact PostgreSQL wire type. Catalog
    /// columns are resolved by stable identity; positional ranges carry their
    /// wire types in the stored query itself.
    pub fn validate_output_pg_types(
        &self,
        resolver: &mut impl FnMut(TableId, ColumnObjectId) -> crate::Result<PgType>,
    ) -> crate::Result<()> {
        validate_query_output_pg_types(self, resolver)
    }
}

fn validate_query_output_pg_types(
    query: &StoredQueryV1,
    resolver: &mut impl FnMut(TableId, ColumnObjectId) -> crate::Result<PgType>,
) -> crate::Result<()> {
    match &query.body {
        StoredQueryBody::Select(select) => {
            let mut ranges = BTreeMap::new();
            if let Some(from) = &select.from {
                collect_ranges(from, false, &mut ranges, 0)?;
                validate_from_output_pg_types(from, resolver)?;
            }
            for (item, output) in select.columns.iter().zip(&select.output_schema) {
                let expected = expression_output_pg_type(&item.expr, &ranges, resolver)?;
                if output.pg_type != expected {
                    return Err(crate::DbError::internal(
                        "stored SELECT output has inconsistent PostgreSQL type metadata",
                    ));
                }
                validate_expr_output_pg_types(&item.expr, resolver)?;
            }
            if let Some(StoredDistinct::On(expressions)) = &select.distinct {
                for expr in expressions {
                    validate_expr_output_pg_types(expr, resolver)?;
                }
            }
            if let Some(expr) = &select.filter {
                validate_expr_output_pg_types(expr, resolver)?;
            }
            for expr in &select.group_by {
                validate_expr_output_pg_types(expr, resolver)?;
            }
            if let Some(expr) = &select.having {
                validate_expr_output_pg_types(expr, resolver)?;
            }
        }
        StoredQueryBody::Values(values) => {
            if values
                .output_schema
                .iter()
                .any(|output| output.pg_type != PgType::from(&output.data_type))
            {
                return Err(crate::DbError::internal(
                    "stored VALUES output has inconsistent PostgreSQL type metadata",
                ));
            }
            for row in &values.rows {
                for expr in row {
                    validate_expr_output_pg_types(expr, resolver)?;
                }
            }
        }
        StoredQueryBody::SetOp(set_op) => {
            if set_op
                .output_schema
                .iter()
                .any(|output| output.pg_type != PgType::from(&output.data_type))
            {
                return Err(crate::DbError::internal(
                    "stored set-operation output has inconsistent PostgreSQL type metadata",
                ));
            }
            validate_query_output_pg_types(&set_op.left, resolver)?;
            validate_query_output_pg_types(&set_op.right, resolver)?;
        }
    }
    for order in &query.order_by {
        validate_expr_output_pg_types(&order.expr, resolver)?;
    }
    for correlation in &query.correlations {
        validate_expr_output_pg_types(&correlation.outer, resolver)?;
    }
    Ok(())
}

fn expression_output_pg_type(
    expr: &StoredQueryExpr,
    ranges: &BTreeMap<BindingId, StoredRangeTarget>,
    resolver: &mut impl FnMut(TableId, ColumnObjectId) -> crate::Result<PgType>,
) -> crate::Result<PgType> {
    match expr {
        StoredQueryExpr::InputRef { range, column, .. } => {
            let target = ranges.get(range).ok_or_else(|| {
                crate::DbError::internal("stored query output references unknown range")
            })?;
            match (target, column) {
                (
                    StoredRangeTarget::Catalog { table, .. },
                    StoredColumnReference::Catalog(column),
                ) => resolver(*table, *column),
                (
                    StoredRangeTarget::Position(columns),
                    StoredColumnReference::Position(position),
                ) => {
                    let position = usize::try_from(*position).map_err(|_| {
                        crate::DbError::internal(
                            "stored query output column position exceeds platform range",
                        )
                    })?;
                    columns
                        .get(position)
                        .map(|column| column.pg_type.clone())
                        .ok_or_else(|| {
                            crate::DbError::internal(
                                "stored query output column position is out of range",
                            )
                        })
                }
                _ => Err(crate::DbError::internal(
                    "stored query output column reference kind does not match range",
                )),
            }
        }
        StoredQueryExpr::Cast { pg_type, .. } => Ok(pg_type.clone()),
        StoredQueryExpr::Function {
            pg_type: Some(pg_type),
            ..
        } => Ok(pg_type.clone()),
        _ => Ok(PgType::from(expr.data_type())),
    }
}

fn validate_from_output_pg_types(
    from: &StoredFrom,
    resolver: &mut impl FnMut(TableId, ColumnObjectId) -> crate::Result<PgType>,
) -> crate::Result<()> {
    match from {
        StoredFrom::Derived { query, .. } => validate_query_output_pg_types(query, resolver),
        StoredFrom::TableFunction { args, .. } => {
            for arg in args {
                validate_expr_output_pg_types(arg, resolver)?;
            }
            Ok(())
        }
        StoredFrom::Join {
            left,
            right,
            condition,
            ..
        } => {
            validate_from_output_pg_types(left, resolver)?;
            validate_from_output_pg_types(right, resolver)?;
            if let Some(expr) = condition {
                validate_expr_output_pg_types(expr, resolver)?;
            }
            Ok(())
        }
        StoredFrom::Table { .. } | StoredFrom::System { .. } => Ok(()),
    }
}

fn validate_expr_output_pg_types(
    expr: &StoredQueryExpr,
    resolver: &mut impl FnMut(TableId, ColumnObjectId) -> crate::Result<PgType>,
) -> crate::Result<()> {
    match expr {
        StoredQueryExpr::ScalarSubquery { query, .. } | StoredQueryExpr::Exists { query, .. } => {
            validate_query_output_pg_types(query, resolver)
        }
        StoredQueryExpr::InSubquery { expr, query, .. } => {
            validate_expr_output_pg_types(expr, resolver)?;
            validate_query_output_pg_types(query, resolver)
        }
        StoredQueryExpr::Binary { left, right, .. }
        | StoredQueryExpr::Any {
            left, array: right, ..
        } => {
            validate_expr_output_pg_types(left, resolver)?;
            validate_expr_output_pg_types(right, resolver)
        }
        StoredQueryExpr::Unary { expr, .. }
        | StoredQueryExpr::IsNull { expr, .. }
        | StoredQueryExpr::IsNotNull { expr, .. }
        | StoredQueryExpr::Cast { expr, .. } => validate_expr_output_pg_types(expr, resolver),
        StoredQueryExpr::Function { args, .. } => {
            for arg in args {
                validate_expr_output_pg_types(arg, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::Array { elements, .. } => {
            for element in elements {
                validate_expr_output_pg_types(element, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            validate_expr_output_pg_types(array, resolver)?;
            for subscript in subscripts {
                validate_expr_output_pg_types(subscript, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::Setval {
            value, is_called, ..
        } => {
            validate_expr_output_pg_types(value, resolver)?;
            if let Some(is_called) = is_called {
                validate_expr_output_pg_types(is_called, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::Aggregate { arg, .. } => {
            if let Some(arg) = arg {
                validate_expr_output_pg_types(arg, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::InList { expr, list, .. } => {
            validate_expr_output_pg_types(expr, resolver)?;
            for item in list {
                validate_expr_output_pg_types(item, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::Between {
            expr, low, high, ..
        } => {
            validate_expr_output_pg_types(expr, resolver)?;
            validate_expr_output_pg_types(low, resolver)?;
            validate_expr_output_pg_types(high, resolver)
        }
        StoredQueryExpr::Like { expr, pattern, .. } => {
            validate_expr_output_pg_types(expr, resolver)?;
            validate_expr_output_pg_types(pattern, resolver)
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                validate_expr_output_pg_types(operand, resolver)?;
            }
            for (when, then) in when_clauses {
                validate_expr_output_pg_types(when, resolver)?;
                validate_expr_output_pg_types(then, resolver)?;
            }
            if let Some(else_clause) = else_clause {
                validate_expr_output_pg_types(else_clause, resolver)?;
            }
            Ok(())
        }
        StoredQueryExpr::Literal { .. }
        | StoredQueryExpr::InputRef { .. }
        | StoredQueryExpr::LocalRef { .. }
        | StoredQueryExpr::OuterRef { .. }
        | StoredQueryExpr::Nextval { .. }
        | StoredQueryExpr::Currval { .. } => Ok(()),
    }
}

impl StoredQueryV1 {
    pub fn for_each_system_relation_reference(
        &self,
        visitor: &mut impl FnMut(i64, &[StoredRangeColumn]),
    ) -> crate::Result<()> {
        collect_query_references(self, None, &mut |_, _, _, _, _| {}, visitor)
    }
}

pub fn validate_stored_query_shape(query: &StoredQueryV1) -> crate::Result<()> {
    query.referenced_catalog_objects().map(|_| ())
}

fn count_node(nodes: &mut usize) -> crate::Result<()> {
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| crate::DbError::internal("stored query node count overflow"))?;
    if *nodes > MAX_STORED_QUERY_NODES {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query exceeds node limit",
        ));
    }
    Ok(())
}

fn check_list(len: usize) -> crate::Result<()> {
    if len > MAX_STORED_QUERY_LIST_ITEMS {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query list exceeds limit",
        ));
    }
    Ok(())
}

fn check_columns(len: usize) -> crate::Result<()> {
    if len > MAX_STORED_QUERY_COLUMNS {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query column list exceeds limit",
        ));
    }
    Ok(())
}

fn require_boolean_expression(expr: &StoredQueryExpr, context: &str) -> crate::Result<()> {
    if expr.data_type() != &DataType::Boolean {
        return Err(crate::DbError::internal(format!(
            "stored query {context} expression is not boolean"
        )));
    }
    Ok(())
}

#[derive(Clone)]
enum StoredRangeTarget {
    Catalog { table: TableId, null_extended: bool },
    Position(Vec<StoredRangeColumn>),
}

struct StoredExprScope<'a> {
    ranges: &'a BTreeMap<BindingId, StoredRangeTarget>,
    output: &'a [StoredQueryColumn],
    correlations: &'a [StoredCorrelatedColumn],
}

fn visit_query(
    query: &StoredQueryV1,
    parent: Option<&StoredExprScope<'_>>,
    depth: usize,
    nodes: &mut usize,
    objects: &mut BTreeSet<crate::CatalogObjectId>,
) -> crate::Result<()> {
    count_node(nodes)?;
    if query.version != STORED_QUERY_VERSION {
        return Err(crate::DbError::internal(format!(
            "unsupported stored query version {}",
            query.version
        )));
    }
    if depth >= MAX_STORED_QUERY_DEPTH {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query exceeds nesting limit",
        ));
    }
    check_list(query.order_by.len())?;
    check_list(query.correlations.len())?;
    if parent.is_none() && !query.correlations.is_empty() {
        return Err(crate::DbError::internal(
            "stored query has correlations without an enclosing scope",
        ));
    }
    let mut ranges = BTreeMap::new();
    if let StoredQueryBody::Select(select) = &query.body
        && let Some(from) = &select.from
    {
        collect_ranges(from, false, &mut ranges, depth + 1)?;
    }
    let scope = StoredExprScope {
        ranges: &ranges,
        output: query.output_schema(),
        correlations: &query.correlations,
    };
    if scope
        .output
        .iter()
        .any(|column| column.pg_type.data_type() != column.data_type)
    {
        return Err(crate::DbError::internal(
            "stored query output has inconsistent PostgreSQL type metadata",
        ));
    }
    match &query.body {
        StoredQueryBody::Select(select) => visit_select(select, &scope, depth, nodes, objects)?,
        StoredQueryBody::Values(values) => {
            check_list(values.rows.len())?;
            check_columns(values.output_schema.len())?;
            if values.rows.is_empty() || values.output_schema.is_empty() {
                return Err(crate::DbError::internal(
                    "stored VALUES query must contain rows and columns",
                ));
            }
            for row in &values.rows {
                check_columns(row.len())?;
                if row.len() != values.output_schema.len() {
                    return Err(crate::DbError::internal(
                        "stored VALUES row width does not match output schema",
                    ));
                }
                for (expr, output) in row.iter().zip(&values.output_schema) {
                    if expr.data_type() != &output.data_type {
                        return Err(crate::DbError::internal(
                            "stored VALUES expression type does not match output schema",
                        ));
                    }
                    validate_expr_placement(expr, ExprPlacement::VALUE)?;
                    visit_expr(expr, &scope, depth, nodes, objects)?;
                }
            }
        }
        StoredQueryBody::SetOp(set_op) => {
            check_columns(set_op.output_schema.len())?;
            let matches_output = |query: &StoredQueryV1| {
                query.output_schema().len() == set_op.output_schema.len()
                    && query
                        .output_schema()
                        .iter()
                        .zip(&set_op.output_schema)
                        .all(|(arm, output)| arm.data_type == output.data_type)
            };
            if set_op.output_schema.is_empty()
                || !matches_output(&set_op.left)
                || !matches_output(&set_op.right)
            {
                return Err(crate::DbError::internal(
                    "stored set-operation schema does not match its arms",
                ));
            }
            visit_query(&set_op.left, None, depth + 1, nodes, objects)?;
            visit_query(&set_op.right, None, depth + 1, nodes, objects)?;
        }
    }
    for order in &query.order_by {
        let placement = if matches!(query.body, StoredQueryBody::Select(_)) {
            ExprPlacement::ORDER_BY_SELECT
        } else {
            ExprPlacement::ORDER_BY_OUTPUT
        };
        validate_expr_placement(&order.expr, placement)?;
        visit_expr(&order.expr, &scope, depth, nodes, objects)?;
    }
    if let StoredQueryBody::Select(select) = &query.body {
        validate_select_semantics(select, &query.order_by)?;
    }
    let correlation_scope = parent.unwrap_or(&scope);
    for correlation in &query.correlations {
        if !matches!(
            correlation.outer,
            StoredQueryExpr::InputRef { .. } | StoredQueryExpr::OuterRef { .. }
        ) {
            return Err(crate::DbError::internal(
                "stored query correlation source is not an outer column reference",
            ));
        }
        visit_expr(&correlation.outer, correlation_scope, depth, nodes, objects)?;
        if correlation.outer.data_type() != &correlation.data_type
            || correlation.outer.nullable() != correlation.nullable
        {
            return Err(crate::DbError::internal(
                "stored query correlation has inconsistent metadata",
            ));
        }
    }
    if let Some(lock) = &query.row_lock {
        let StoredQueryBody::Select(select) = &query.body else {
            return Err(crate::DbError::internal(
                "stored query row lock requires a SELECT body",
            ));
        };
        if !matches!(select.from.as_ref(), Some(StoredFrom::Table { table, .. }) if *table == lock.table)
            || select.distinct.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.columns.iter().any(|item| {
                expr_contains_aggregate(&item.expr) || expr_contains_subquery(&item.expr)
            })
            || select.filter.as_ref().is_some_and(expr_contains_subquery)
            || query.order_by.iter().any(|item| {
                expr_contains_aggregate(&item.expr) || expr_contains_subquery(&item.expr)
            })
        {
            return Err(crate::DbError::internal(
                "stored query row lock violates locking-query restrictions",
            ));
        }
        objects.insert(crate::CatalogObjectId::Table(lock.table));
    }
    Ok(())
}

fn collect_ranges(
    from: &StoredFrom,
    null_extended: bool,
    ranges: &mut BTreeMap<BindingId, StoredRangeTarget>,
    depth: usize,
) -> crate::Result<()> {
    if depth >= MAX_STORED_QUERY_DEPTH {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query FROM clause exceeds nesting limit",
        ));
    }
    let (range, target) = match from {
        StoredFrom::Table { table, range, .. } => (
            Some(*range),
            Some(StoredRangeTarget::Catalog {
                table: *table,
                null_extended,
            }),
        ),
        StoredFrom::System { range, schema, .. }
        | StoredFrom::Derived { range, schema, .. }
        | StoredFrom::TableFunction { range, schema, .. } => {
            let mut columns = schema.clone();
            if null_extended {
                for column in &mut columns {
                    column.nullable = true;
                }
            }
            (Some(*range), Some(StoredRangeTarget::Position(columns)))
        }
        StoredFrom::Join {
            left,
            right,
            join_type,
            ..
        } => {
            collect_ranges(
                left,
                null_extended || matches!(join_type, StoredJoinType::Right | StoredJoinType::Full),
                ranges,
                depth + 1,
            )?;
            collect_ranges(
                right,
                null_extended || matches!(join_type, StoredJoinType::Left | StoredJoinType::Full),
                ranges,
                depth + 1,
            )?;
            (None, None)
        }
    };
    if let Some(StoredRangeTarget::Position(columns)) = &target
        && columns
            .iter()
            .any(|column| column.pg_type.data_type() != column.data_type)
    {
        return Err(crate::DbError::internal(
            "stored query range has inconsistent PostgreSQL type metadata",
        ));
    }
    if let (Some(range), Some(target)) = (range, target)
        && ranges.insert(range, target).is_some()
    {
        return Err(crate::DbError::internal(
            "stored query contains duplicate range id",
        ));
    }
    Ok(())
}

fn visit_select(
    select: &StoredSelect,
    scope: &StoredExprScope<'_>,
    depth: usize,
    nodes: &mut usize,
    objects: &mut BTreeSet<crate::CatalogObjectId>,
) -> crate::Result<()> {
    check_columns(select.columns.len())?;
    check_list(select.group_by.len())?;
    check_columns(select.output_schema.len())?;
    if select.columns.is_empty() || select.columns.len() != select.output_schema.len() {
        return Err(crate::DbError::internal(
            "stored SELECT list does not match output schema",
        ));
    }
    for (item, output) in select.columns.iter().zip(&select.output_schema) {
        if item.alias != output.name || item.expr.data_type() != &output.data_type {
            return Err(crate::DbError::internal(
                "stored SELECT item does not match output schema",
            ));
        }
        validate_expr_placement(&item.expr, ExprPlacement::SELECT_ITEM)?;
    }
    if let Some(StoredDistinct::On(exprs)) = &select.distinct {
        check_list(exprs.len())?;
        for expr in exprs {
            validate_expr_placement(expr, ExprPlacement::NON_AGGREGATE)?;
            visit_expr(expr, scope, depth, nodes, objects)?;
        }
    }
    if let Some(from) = &select.from {
        let mut available = BTreeMap::new();
        visit_from(from, scope, &mut available, depth + 1, nodes, objects)?;
        if available.len() != scope.ranges.len() {
            return Err(crate::DbError::internal(
                "stored query FROM validation did not visit every range",
            ));
        }
    }
    for item in &select.columns {
        visit_expr(&item.expr, scope, depth, nodes, objects)?;
    }
    if let Some(expr) = &select.filter {
        require_boolean_expression(expr, "WHERE")?;
        validate_expr_placement(expr, ExprPlacement::NON_AGGREGATE)?;
        visit_expr(expr, scope, depth, nodes, objects)?;
    }
    for expr in &select.group_by {
        validate_expr_placement(expr, ExprPlacement::NON_AGGREGATE)?;
        visit_expr(expr, scope, depth, nodes, objects)?;
    }
    if let Some(expr) = &select.having {
        require_boolean_expression(expr, "HAVING")?;
        validate_expr_placement(expr, ExprPlacement::SELECT_ITEM)?;
        visit_expr(expr, scope, depth, nodes, objects)?;
    }
    Ok(())
}

fn visit_from(
    from: &StoredFrom,
    scope: &StoredExprScope<'_>,
    available: &mut BTreeMap<BindingId, StoredRangeTarget>,
    depth: usize,
    nodes: &mut usize,
    objects: &mut BTreeSet<crate::CatalogObjectId>,
) -> crate::Result<()> {
    count_node(nodes)?;
    if depth >= MAX_STORED_QUERY_DEPTH {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query FROM clause exceeds nesting limit",
        ));
    }
    match from {
        StoredFrom::Table { table, range, .. } => {
            objects.insert(crate::CatalogObjectId::Table(*table));
            add_available_range(*range, scope, available)?;
        }
        StoredFrom::System {
            relation_oid,
            range,
            ..
        } => {
            objects.insert(crate::CatalogObjectId::SystemRelation(*relation_oid));
            add_available_range(*range, scope, available)?;
        }
        StoredFrom::Derived {
            query,
            range,
            schema,
            lateral,
            ..
        } => {
            check_columns(schema.len())?;
            let output_nullability = query.output_nullability()?;
            if schema.len() != query.output_schema().len()
                || schema.len() != output_nullability.len()
                || schema
                    .iter()
                    .zip(query.output_schema().iter().zip(output_nullability))
                    .any(|(range, (output, nullable))| {
                        range.data_type != output.data_type
                            || range.pg_type != output.pg_type
                            || range.nullable != nullable
                    })
            {
                return Err(crate::DbError::internal(
                    "stored derived range schema does not match query output",
                ));
            }
            if !lateral && !query.correlations.is_empty() {
                return Err(crate::DbError::internal(
                    "stored non-LATERAL derived query contains correlations",
                ));
            }
            if *lateral {
                let preceding_scope = StoredExprScope {
                    ranges: available,
                    output: scope.output,
                    correlations: scope.correlations,
                };
                visit_query(query, Some(&preceding_scope), depth + 1, nodes, objects)?;
            } else {
                visit_query(query, None, depth + 1, nodes, objects)?;
            }
            add_available_range(*range, scope, available)?;
        }
        StoredFrom::TableFunction {
            function,
            args,
            range,
            schema,
            ..
        } => {
            let valid = match *function {
                UNNEST_FUNCTION_ID => {
                    matches!(args.as_slice(), [arg] if matches!(arg.data_type(), DataType::Array(array) if schema.len() == 1 && schema[0].data_type == *array.element_type() && schema[0].pg_type == PgType::from(array.element_type()) && schema[0].nullable))
                }
                GENERATE_SERIES_FUNCTION_ID => {
                    (2..=3).contains(&args.len())
                        && args.iter().all(|arg| arg.data_type() == &DataType::Integer)
                        && matches!(schema.as_slice(), [column] if column.data_type == DataType::Integer && column.pg_type == PgType::from(&DataType::Integer) && !column.nullable)
                }
                _ => false,
            };
            if !valid {
                return Err(crate::DbError::internal(format!(
                    "stored query table function {function} has invalid arguments or result schema"
                )));
            }
            check_list(args.len())?;
            check_columns(schema.len())?;
            objects.insert(crate::CatalogObjectId::Function(*function));
            let preceding_scope = StoredExprScope {
                ranges: available,
                output: scope.output,
                correlations: scope.correlations,
            };
            for arg in args {
                validate_expr_placement(arg, ExprPlacement::TABLE_FUNCTION)?;
                visit_expr(arg, &preceding_scope, depth + 1, nodes, objects)?;
            }
            add_available_range(*range, scope, available)?;
        }
        StoredFrom::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let condition_is_valid = match join_type {
                StoredJoinType::Cross => condition.is_none(),
                StoredJoinType::Inner
                | StoredJoinType::Left
                | StoredJoinType::Right
                | StoredJoinType::Full => condition.is_some(),
            };
            if !condition_is_valid {
                return Err(crate::DbError::internal(
                    "stored query join condition does not match join type",
                ));
            }
            visit_from(left, scope, available, depth + 1, nodes, objects)?;
            visit_from(right, scope, available, depth + 1, nodes, objects)?;
            if let Some(expr) = condition {
                require_boolean_expression(expr, "JOIN")?;
                validate_expr_placement(expr, ExprPlacement::NON_AGGREGATE)?;
                let mut condition_ranges = available.clone();
                let mut current_join_ranges = BTreeMap::new();
                collect_ranges(left, false, &mut current_join_ranges, depth + 1)?;
                collect_ranges(right, false, &mut current_join_ranges, depth + 1)?;
                condition_ranges.extend(current_join_ranges);
                let joined_scope = StoredExprScope {
                    ranges: &condition_ranges,
                    output: scope.output,
                    correlations: scope.correlations,
                };
                visit_expr(expr, &joined_scope, depth + 1, nodes, objects)?;
            }
        }
    }
    Ok(())
}

fn add_available_range(
    range: BindingId,
    complete: &StoredExprScope<'_>,
    available: &mut BTreeMap<BindingId, StoredRangeTarget>,
) -> crate::Result<()> {
    let target = complete.ranges.get(&range).cloned().ok_or_else(|| {
        crate::DbError::internal("stored query FROM range is missing from complete scope")
    })?;
    if available.insert(range, target).is_some() {
        return Err(crate::DbError::internal(
            "stored query FROM range was visited more than once",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ExprPlacement {
    allow_aggregate: bool,
    allow_subquery: bool,
    allow_local_root: bool,
    require_local_root: bool,
}

impl ExprPlacement {
    const SELECT_ITEM: Self = Self {
        allow_aggregate: true,
        allow_subquery: true,
        allow_local_root: false,
        require_local_root: false,
    };
    const NON_AGGREGATE: Self = Self {
        allow_aggregate: false,
        allow_subquery: true,
        allow_local_root: false,
        require_local_root: false,
    };
    const TABLE_FUNCTION: Self = Self {
        allow_aggregate: false,
        allow_subquery: false,
        allow_local_root: false,
        require_local_root: false,
    };
    const VALUE: Self = Self::NON_AGGREGATE;
    const ORDER_BY_SELECT: Self = Self::SELECT_ITEM;
    const ORDER_BY_OUTPUT: Self = Self {
        allow_aggregate: false,
        allow_subquery: false,
        allow_local_root: true,
        require_local_root: true,
    };
}

fn validate_expr_placement(expr: &StoredQueryExpr, placement: ExprPlacement) -> crate::Result<()> {
    if placement.require_local_root && !matches!(expr, StoredQueryExpr::LocalRef { .. }) {
        return Err(crate::DbError::internal(
            "stored query output ORDER BY is not a local output reference",
        ));
    }
    validate_expr_placement_inner(expr, placement, true)
}

fn validate_expr_placement_inner(
    expr: &StoredQueryExpr,
    placement: ExprPlacement,
    root: bool,
) -> crate::Result<()> {
    let child = |expr| validate_expr_placement_inner(expr, placement, false);
    match expr {
        StoredQueryExpr::LocalRef { .. } => {
            if !root || !placement.allow_local_root {
                return Err(crate::DbError::internal(
                    "stored query local output reference appears outside output ORDER BY",
                ));
            }
        }
        StoredQueryExpr::Aggregate { arg, .. } => {
            if !placement.allow_aggregate {
                return Err(crate::DbError::internal(
                    "stored query aggregate appears in a non-aggregate expression context",
                ));
            }
            if let Some(arg) = arg {
                let nested = ExprPlacement {
                    allow_aggregate: false,
                    ..placement
                };
                validate_expr_placement_inner(arg, nested, false)?;
            }
        }
        StoredQueryExpr::ScalarSubquery { .. } | StoredQueryExpr::Exists { .. } => {
            if !placement.allow_subquery {
                return Err(crate::DbError::internal(
                    "stored query subquery appears in an unsupported expression context",
                ));
            }
        }
        StoredQueryExpr::InSubquery { expr, .. } => {
            if !placement.allow_subquery {
                return Err(crate::DbError::internal(
                    "stored query subquery appears in an unsupported expression context",
                ));
            }
            child(expr)?;
        }
        StoredQueryExpr::Binary { left, right, .. } => {
            child(left)?;
            child(right)?;
        }
        StoredQueryExpr::Unary { expr, .. }
        | StoredQueryExpr::IsNull { expr, .. }
        | StoredQueryExpr::IsNotNull { expr, .. }
        | StoredQueryExpr::Cast { expr, .. } => child(expr)?,
        StoredQueryExpr::Function { args, .. } => {
            for arg in args {
                child(arg)?;
            }
        }
        StoredQueryExpr::Array { elements, .. } => {
            for element in elements {
                child(element)?;
            }
        }
        StoredQueryExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            child(array)?;
            for subscript in subscripts {
                child(subscript)?;
            }
        }
        StoredQueryExpr::Any { left, array, .. } => {
            child(left)?;
            child(array)?;
        }
        StoredQueryExpr::Setval {
            value, is_called, ..
        } => {
            child(value)?;
            if let Some(is_called) = is_called {
                child(is_called)?;
            }
        }
        StoredQueryExpr::InList { expr, list, .. } => {
            child(expr)?;
            for item in list {
                child(item)?;
            }
        }
        StoredQueryExpr::Between {
            expr, low, high, ..
        } => {
            child(expr)?;
            child(low)?;
            child(high)?;
        }
        StoredQueryExpr::Like { expr, pattern, .. } => {
            child(expr)?;
            child(pattern)?;
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                child(operand)?;
            }
            for (when, then) in when_clauses {
                child(when)?;
                child(then)?;
            }
            if let Some(else_clause) = else_clause {
                child(else_clause)?;
            }
        }
        StoredQueryExpr::Literal { .. }
        | StoredQueryExpr::InputRef { .. }
        | StoredQueryExpr::OuterRef { .. }
        | StoredQueryExpr::Nextval { .. }
        | StoredQueryExpr::Currval { .. } => {}
    }
    Ok(())
}

fn expr_contains_aggregate(expr: &StoredQueryExpr) -> bool {
    expr_features(expr).0
}

fn expr_contains_subquery(expr: &StoredQueryExpr) -> bool {
    expr_features(expr).1
}

fn validate_select_semantics(
    select: &StoredSelect,
    order_by: &[StoredOrderBy],
) -> crate::Result<()> {
    match &select.distinct {
        Some(StoredDistinct::All)
            if order_by
                .iter()
                .any(|item| !select.columns.iter().any(|column| column.expr == item.expr)) =>
        {
            return Err(crate::DbError::internal(
                "stored SELECT DISTINCT orders by an expression outside its select list",
            ));
        }
        Some(StoredDistinct::All) => {}
        Some(StoredDistinct::On(keys)) => validate_distinct_on_order_by(keys, order_by)?,
        None => {}
    }

    let aggregate_context = !select.group_by.is_empty()
        || select
            .columns
            .iter()
            .any(|item| expr_contains_aggregate(&item.expr))
        || select.having.is_some()
        || order_by
            .iter()
            .any(|item| expr_contains_aggregate(&item.expr))
        || matches!(
            &select.distinct,
            Some(StoredDistinct::On(keys))
                if keys.iter().any(expr_contains_aggregate)
        );
    if !aggregate_context {
        return Ok(());
    }
    for item in &select.columns {
        validate_grouped_expr(&item.expr, &select.group_by)?;
    }
    if let Some(having) = &select.having {
        validate_grouped_expr(having, &select.group_by)?;
    }
    for item in order_by {
        validate_grouped_expr(&item.expr, &select.group_by)?;
    }
    if let Some(StoredDistinct::On(keys)) = &select.distinct {
        for key in keys {
            validate_grouped_expr(key, &select.group_by)?;
        }
    }
    Ok(())
}

fn validate_distinct_on_order_by(
    keys: &[StoredQueryExpr],
    order_by: &[StoredOrderBy],
) -> crate::Result<()> {
    let mut distinct_keys = Vec::new();
    for key in keys {
        if !distinct_keys.contains(&key) {
            distinct_keys.push(key);
        }
    }
    let mut matched_keys = Vec::new();
    for item in order_by {
        match keys.iter().find(|key| **key == item.expr) {
            Some(key) => {
                if !matched_keys.contains(&key) {
                    matched_keys.push(key);
                }
            }
            None => {
                if matched_keys.len() < distinct_keys.len() {
                    return Err(crate::DbError::internal(
                        "stored SELECT DISTINCT ON keys do not match leading ORDER BY expressions",
                    ));
                }
                break;
            }
        }
    }
    Ok(())
}

fn validate_grouped_expr(
    expr: &StoredQueryExpr,
    group_by: &[StoredQueryExpr],
) -> crate::Result<()> {
    if matches!(expr, StoredQueryExpr::Aggregate { .. }) {
        return Ok(());
    }
    match expr {
        StoredQueryExpr::ScalarSubquery { query, .. } | StoredQueryExpr::Exists { query, .. } => {
            return validate_grouped_correlations(query, group_by);
        }
        StoredQueryExpr::InSubquery {
            expr: operand,
            query,
            ..
        } => {
            validate_grouped_expr(operand, group_by)?;
            return validate_grouped_correlations(query, group_by);
        }
        _ => {}
    }
    if !expr_contains_aggregate(expr) {
        if !references_current_input(expr) || group_by.iter().any(|group| group == expr) {
            return Ok(());
        }
        return Err(crate::DbError::internal(
            "stored non-aggregate expression does not appear exactly in GROUP BY",
        ));
    }
    match expr {
        StoredQueryExpr::Binary { left, right, .. }
        | StoredQueryExpr::Any {
            left, array: right, ..
        } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(right, group_by)
        }
        StoredQueryExpr::Unary { expr, .. }
        | StoredQueryExpr::IsNull { expr, .. }
        | StoredQueryExpr::IsNotNull { expr, .. }
        | StoredQueryExpr::Cast { expr, .. } => validate_grouped_expr(expr, group_by),
        StoredQueryExpr::Function { args, .. } => {
            for arg in args {
                validate_grouped_expr(arg, group_by)?;
            }
            Ok(())
        }
        StoredQueryExpr::Array { elements, .. } => {
            for element in elements {
                validate_grouped_expr(element, group_by)?;
            }
            Ok(())
        }
        StoredQueryExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            validate_grouped_expr(array, group_by)?;
            for subscript in subscripts {
                validate_grouped_expr(subscript, group_by)?;
            }
            Ok(())
        }
        StoredQueryExpr::Setval {
            value, is_called, ..
        } => {
            validate_grouped_expr(value, group_by)?;
            if let Some(is_called) = is_called {
                validate_grouped_expr(is_called, group_by)?;
            }
            Ok(())
        }
        StoredQueryExpr::InList { expr, list, .. } => {
            validate_grouped_expr(expr, group_by)?;
            for item in list {
                validate_grouped_expr(item, group_by)?;
            }
            Ok(())
        }
        StoredQueryExpr::Between {
            expr, low, high, ..
        } => {
            validate_grouped_expr(expr, group_by)?;
            validate_grouped_expr(low, group_by)?;
            validate_grouped_expr(high, group_by)
        }
        StoredQueryExpr::Like { expr, pattern, .. } => {
            validate_grouped_expr(expr, group_by)?;
            validate_grouped_expr(pattern, group_by)
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                validate_grouped_expr(operand, group_by)?;
            }
            for (when, then) in when_clauses {
                validate_grouped_expr(when, group_by)?;
                validate_grouped_expr(then, group_by)?;
            }
            if let Some(else_clause) = else_clause {
                validate_grouped_expr(else_clause, group_by)?;
            }
            Ok(())
        }
        StoredQueryExpr::Literal { .. }
        | StoredQueryExpr::InputRef { .. }
        | StoredQueryExpr::LocalRef { .. }
        | StoredQueryExpr::OuterRef { .. }
        | StoredQueryExpr::Nextval { .. }
        | StoredQueryExpr::Currval { .. }
        | StoredQueryExpr::Aggregate { .. }
        | StoredQueryExpr::ScalarSubquery { .. }
        | StoredQueryExpr::Exists { .. }
        | StoredQueryExpr::InSubquery { .. } => Ok(()),
    }
}

fn validate_grouped_correlations(
    query: &StoredQueryV1,
    group_by: &[StoredQueryExpr],
) -> crate::Result<()> {
    for correlation in &query.correlations {
        validate_grouped_expr(&correlation.outer, group_by)?;
    }
    Ok(())
}

fn references_current_input(expr: &StoredQueryExpr) -> bool {
    match expr {
        StoredQueryExpr::InputRef { .. } => true,
        StoredQueryExpr::Binary { left, right, .. }
        | StoredQueryExpr::Any {
            left, array: right, ..
        } => references_current_input(left) || references_current_input(right),
        StoredQueryExpr::Unary { expr, .. }
        | StoredQueryExpr::IsNull { expr, .. }
        | StoredQueryExpr::IsNotNull { expr, .. }
        | StoredQueryExpr::Cast { expr, .. } => references_current_input(expr),
        StoredQueryExpr::Function { args, .. } => args.iter().any(references_current_input),
        StoredQueryExpr::Array { elements, .. } => elements.iter().any(references_current_input),
        StoredQueryExpr::ArraySubscript {
            array, subscripts, ..
        } => references_current_input(array) || subscripts.iter().any(references_current_input),
        StoredQueryExpr::Setval {
            value, is_called, ..
        } => {
            references_current_input(value)
                || is_called.as_deref().is_some_and(references_current_input)
        }
        StoredQueryExpr::Aggregate { arg, .. } => {
            arg.as_deref().is_some_and(references_current_input)
        }
        StoredQueryExpr::InList { expr, list, .. } => {
            references_current_input(expr) || list.iter().any(references_current_input)
        }
        StoredQueryExpr::Between {
            expr, low, high, ..
        } => {
            references_current_input(expr)
                || references_current_input(low)
                || references_current_input(high)
        }
        StoredQueryExpr::Like { expr, pattern, .. } => {
            references_current_input(expr) || references_current_input(pattern)
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(references_current_input)
                || when_clauses.iter().any(|(when, then)| {
                    references_current_input(when) || references_current_input(then)
                })
                || else_clause.as_deref().is_some_and(references_current_input)
        }
        StoredQueryExpr::InSubquery { expr, query, .. } => {
            references_current_input(expr) || correlations_reference_current_input(query)
        }
        StoredQueryExpr::ScalarSubquery { query, .. } | StoredQueryExpr::Exists { query, .. } => {
            correlations_reference_current_input(query)
        }
        StoredQueryExpr::Literal { .. }
        | StoredQueryExpr::LocalRef { .. }
        | StoredQueryExpr::OuterRef { .. }
        | StoredQueryExpr::Nextval { .. }
        | StoredQueryExpr::Currval { .. } => false,
    }
}

fn correlations_reference_current_input(query: &StoredQueryV1) -> bool {
    query
        .correlations
        .iter()
        .any(|correlation| references_current_input(&correlation.outer))
}

fn expr_features(expr: &StoredQueryExpr) -> (bool, bool) {
    let merge = |left: (bool, bool), right: (bool, bool)| (left.0 || right.0, left.1 || right.1);
    match expr {
        StoredQueryExpr::Aggregate { arg, .. } => arg.as_deref().map_or((true, false), |arg| {
            merge((true, false), expr_features(arg))
        }),
        StoredQueryExpr::ScalarSubquery { .. } | StoredQueryExpr::Exists { .. } => (false, true),
        StoredQueryExpr::InSubquery { expr, .. } => merge(expr_features(expr), (false, true)),
        StoredQueryExpr::Binary { left, right, .. }
        | StoredQueryExpr::Any {
            left, array: right, ..
        } => merge(expr_features(left), expr_features(right)),
        StoredQueryExpr::Unary { expr, .. }
        | StoredQueryExpr::IsNull { expr, .. }
        | StoredQueryExpr::IsNotNull { expr, .. }
        | StoredQueryExpr::Cast { expr, .. } => expr_features(expr),
        StoredQueryExpr::Function { args, .. } => args.iter().fold((false, false), |found, arg| {
            merge(found, expr_features(arg))
        }),
        StoredQueryExpr::Array { elements, .. } => {
            elements.iter().fold((false, false), |found, element| {
                merge(found, expr_features(element))
            })
        }
        StoredQueryExpr::ArraySubscript {
            array, subscripts, ..
        } => subscripts
            .iter()
            .fold(expr_features(array), |found, subscript| {
                merge(found, expr_features(subscript))
            }),
        StoredQueryExpr::Setval {
            value, is_called, ..
        } => is_called.as_deref().map_or_else(
            || expr_features(value),
            |is_called| merge(expr_features(value), expr_features(is_called)),
        ),
        StoredQueryExpr::InList { expr, list, .. } => {
            list.iter().fold(expr_features(expr), |found, item| {
                merge(found, expr_features(item))
            })
        }
        StoredQueryExpr::Between {
            expr, low, high, ..
        } => merge(
            merge(expr_features(expr), expr_features(low)),
            expr_features(high),
        ),
        StoredQueryExpr::Like { expr, pattern, .. } => {
            merge(expr_features(expr), expr_features(pattern))
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            let mut found = operand.as_deref().map_or((false, false), expr_features);
            for (when, then) in when_clauses {
                found = merge(found, expr_features(when));
                found = merge(found, expr_features(then));
            }
            else_clause
                .as_deref()
                .map_or(found, |expr| merge(found, expr_features(expr)))
        }
        StoredQueryExpr::Literal { .. }
        | StoredQueryExpr::InputRef { .. }
        | StoredQueryExpr::LocalRef { .. }
        | StoredQueryExpr::OuterRef { .. }
        | StoredQueryExpr::Nextval { .. }
        | StoredQueryExpr::Currval { .. } => (false, false),
    }
}

fn visit_expr(
    expr: &StoredQueryExpr,
    scope: &StoredExprScope<'_>,
    depth: usize,
    nodes: &mut usize,
    objects: &mut BTreeSet<crate::CatalogObjectId>,
) -> crate::Result<()> {
    count_node(nodes)?;
    if depth >= MAX_STORED_QUERY_DEPTH {
        return Err(crate::DbError::plan(
            crate::SqlState::ProgramLimitExceeded,
            "stored query expression exceeds nesting limit",
        ));
    }
    match expr {
        StoredQueryExpr::Literal {
            value,
            data_type,
            nullable,
        } => {
            if !crate::value_matches_type(value, data_type)
                || *nullable != matches!(value, Value::Null)
                || !crate::value_is_finite(value)
            {
                return Err(crate::DbError::internal(
                    "stored query literal has invalid type metadata",
                ));
            }
        }
        StoredQueryExpr::LocalRef {
            output, data_type, ..
        } => {
            let output = usize::try_from(*output).map_err(|_| {
                crate::DbError::internal("stored query output reference exceeds platform range")
            })?;
            if scope
                .output
                .get(output)
                .is_none_or(|column| column.data_type != *data_type)
            {
                return Err(crate::DbError::internal(
                    "stored query output reference is out of range or has a stale type",
                ));
            }
        }
        StoredQueryExpr::OuterRef {
            correlation,
            data_type,
            nullable,
        } => {
            let correlation = usize::try_from(*correlation).map_err(|_| {
                crate::DbError::internal(
                    "stored query correlation reference exceeds platform range",
                )
            })?;
            if scope
                .correlations
                .get(correlation)
                .is_none_or(|column| column.data_type != *data_type || column.nullable != *nullable)
            {
                return Err(crate::DbError::internal(
                    "stored query correlation reference is out of range or has stale metadata",
                ));
            }
        }
        StoredQueryExpr::InputRef { range, column, .. } => {
            let table = scope.ranges.get(range).ok_or_else(|| {
                crate::DbError::internal("stored query expression references unknown range")
            })?;
            match (table, column) {
                (
                    StoredRangeTarget::Catalog { table, .. },
                    StoredColumnReference::Catalog(column),
                ) => {
                    objects.insert(crate::CatalogObjectId::Table(*table));
                    objects.insert(crate::CatalogObjectId::Column {
                        relation: *table,
                        column: *column,
                    });
                }
                (
                    StoredRangeTarget::Position(columns),
                    StoredColumnReference::Position(position),
                ) => {
                    let position = usize::try_from(*position).map_err(|_| {
                        crate::DbError::internal(
                            "stored query column position exceeds platform range",
                        )
                    })?;
                    let stored = columns.get(position).ok_or_else(|| {
                        crate::DbError::internal("stored query column position is out of range")
                    })?;
                    if stored.data_type != *expr.data_type() || stored.nullable != expr.nullable() {
                        return Err(crate::DbError::internal(
                            "stored query positional column type does not match its range",
                        ));
                    }
                }
                _ => {
                    return Err(crate::DbError::internal(
                        "stored query column reference kind does not match range",
                    ));
                }
            }
        }
        StoredQueryExpr::Binary {
            left,
            op,
            right,
            data_type,
            nullable,
        } => {
            visit_expr(left, scope, depth + 1, nodes, objects)?;
            visit_expr(right, scope, depth + 1, nodes, objects)?;
            let expected = query_binary_result_type(left.data_type(), *op, right.data_type())
                .ok_or_else(|| {
                    crate::DbError::internal(
                        "stored query binary expression has invalid operand types",
                    )
                })?;
            let expected_nullable = if matches!(
                op,
                StoredQueryBinOp::IsDistinctFrom | StoredQueryBinOp::IsNotDistinctFrom
            ) {
                false
            } else {
                left.nullable() || right.nullable()
            };
            require_query_metadata(data_type, *nullable, &expected, expected_nullable)?;
        }
        StoredQueryExpr::Unary {
            op,
            expr,
            data_type,
            nullable,
        } => {
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            let valid = match op {
                StoredQueryUnaryOp::Neg => {
                    query_numeric_family(expr.data_type()).is_some()
                        || expr.data_type() == &DataType::Interval
                }
                StoredQueryUnaryOp::Not => expr.data_type() == &DataType::Boolean,
            };
            if !valid {
                return Err(crate::DbError::internal(
                    "stored query unary expression has invalid operand type",
                ));
            }
            let expected = match op {
                StoredQueryUnaryOp::Neg => expr.data_type().clone(),
                StoredQueryUnaryOp::Not => DataType::Boolean,
            };
            require_query_metadata(data_type, *nullable, &expected, expr.nullable())?;
        }
        StoredQueryExpr::Function {
            function,
            args,
            data_type,
            pg_type,
            nullable,
        } => {
            check_list(args.len())?;
            let arg_types: Vec<_> = args
                .iter()
                .map(StoredQueryExpr::data_type)
                .cloned()
                .collect();
            if !crate::scalar_function_id_matches(
                *function,
                &arg_types,
                data_type,
                pg_type.as_ref(),
            ) {
                return Err(crate::DbError::internal(format!(
                    "stored query scalar function {function} has invalid arguments or result type"
                )));
            }
            let (registered, _) =
                crate::lookup_scalar_function_by_id(*function).ok_or_else(|| {
                    crate::DbError::internal(format!(
                        "stored query references unknown scalar function {function}"
                    ))
                })?;
            let arguments: Vec<_> = args
                .iter()
                .map(|argument| crate::ArgType {
                    data_type: argument.data_type().clone(),
                    literal: match argument {
                        StoredQueryExpr::Literal { value, .. } => Some(value),
                        _ => None,
                    },
                })
                .collect();
            let signature_result =
                (registered.signature)(registered.name, &arguments).map_err(|_| {
                    crate::DbError::internal(format!(
                        "stored query scalar function {function} has invalid argument values"
                    ))
                })?;
            if signature_result != *data_type {
                return Err(crate::DbError::internal(
                    "stored query scalar function has inconsistent result metadata",
                ));
            }
            let expected_nullable =
                registered.result_nullable(args.iter().map(StoredQueryExpr::nullable));
            if *nullable != expected_nullable {
                return Err(crate::DbError::internal(
                    "stored query scalar function has invalid nullability metadata",
                ));
            }
            objects.insert(crate::CatalogObjectId::Function(*function));
            for arg in args {
                visit_expr(arg, scope, depth + 1, nodes, objects)?;
            }
        }
        StoredQueryExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => {
            check_list(elements.len())?;
            check_list(dimensions.len())?;
            for element in elements {
                visit_expr(element, scope, depth + 1, nodes, objects)?;
            }
            if matches!(element_type, DataType::Array(_))
                || elements
                    .iter()
                    .any(|element| element.data_type() != element_type)
                || !query_array_shape_matches(dimensions, elements.len())?
            {
                return Err(crate::DbError::internal(
                    "stored query array has invalid shape or element type",
                ));
            }
            let expected = DataType::Array(crate::ArrayType::new(element_type.clone())?);
            require_query_metadata(data_type, *nullable, &expected, false)?;
        }
        StoredQueryExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => {
            check_list(subscripts.len())?;
            visit_expr(array, scope, depth + 1, nodes, objects)?;
            for subscript in subscripts {
                visit_expr(subscript, scope, depth + 1, nodes, objects)?;
            }
            let DataType::Array(array_type) = array.data_type() else {
                return Err(crate::DbError::internal(
                    "stored query array subscript has a non-array operand",
                ));
            };
            if subscripts.is_empty()
                || subscripts
                    .iter()
                    .any(|subscript| subscript.data_type() != &DataType::Integer)
            {
                return Err(crate::DbError::internal(
                    "stored query array subscript has invalid indexes",
                ));
            }
            require_query_metadata(data_type, *nullable, array_type.element_type(), true)?;
        }
        StoredQueryExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => {
            visit_expr(left, scope, depth + 1, nodes, objects)?;
            visit_expr(array, scope, depth + 1, nodes, objects)?;
            let DataType::Array(array_type) = array.data_type() else {
                return Err(crate::DbError::internal(
                    "stored query ANY has a non-array operand",
                ));
            };
            if !query_is_comparison(*op) || array_type.element_type() != left.data_type() {
                return Err(crate::DbError::internal(
                    "stored query ANY has invalid operands",
                ));
            }
            require_query_metadata(data_type, *nullable, &DataType::Boolean, true)?;
        }
        StoredQueryExpr::Nextval {
            sequence,
            data_type,
            nullable,
        }
        | StoredQueryExpr::Currval {
            sequence,
            data_type,
            nullable,
        } => {
            require_query_metadata(data_type, *nullable, &DataType::Integer, false)?;
            objects.insert(crate::CatalogObjectId::Sequence(*sequence));
        }
        StoredQueryExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => {
            objects.insert(crate::CatalogObjectId::Sequence(*sequence));
            visit_expr(value, scope, depth + 1, nodes, objects)?;
            if let Some(expr) = is_called {
                visit_expr(expr, scope, depth + 1, nodes, objects)?;
            }
            if value.data_type() != &DataType::Integer
                || is_called
                    .as_deref()
                    .is_some_and(|argument| argument.data_type() != &DataType::Boolean)
            {
                return Err(crate::DbError::internal(
                    "stored query setval has invalid argument types",
                ));
            }
            let expected_nullable =
                value.nullable() || is_called.as_deref().is_some_and(StoredQueryExpr::nullable);
            require_query_metadata(data_type, *nullable, &DataType::Integer, expected_nullable)?;
        }
        StoredQueryExpr::Aggregate {
            function,
            arg,
            distinct,
            data_type,
            nullable,
        } => {
            if !stored_aggregate_function_exists(*function) {
                return Err(crate::DbError::internal(format!(
                    "stored query references unknown aggregate function {function}"
                )));
            }
            objects.insert(crate::CatalogObjectId::Function(*function));
            if let Some(expr) = arg {
                visit_expr(expr, scope, depth + 1, nodes, objects)?;
            }
            validate_query_aggregate(*function, arg.as_deref(), *distinct, data_type, *nullable)?;
        }
        StoredQueryExpr::IsNull {
            expr,
            data_type,
            nullable,
        }
        | StoredQueryExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => {
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            require_query_metadata(data_type, *nullable, &DataType::Boolean, false)?;
        }
        StoredQueryExpr::InList {
            expr,
            list,
            data_type,
            nullable,
            ..
        } => {
            check_list(list.len())?;
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            for item in list {
                visit_expr(item, scope, depth + 1, nodes, objects)?;
            }
            if list.is_empty() || list.iter().any(|item| item.data_type() != expr.data_type()) {
                return Err(crate::DbError::internal(
                    "stored query IN list has invalid item types",
                ));
            }
            let expected_nullable = expr.nullable() || list.iter().any(StoredQueryExpr::nullable);
            require_query_metadata(data_type, *nullable, &DataType::Boolean, expected_nullable)?;
        }
        StoredQueryExpr::Between {
            expr,
            low,
            high,
            data_type,
            nullable,
            ..
        } => {
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            visit_expr(low, scope, depth + 1, nodes, objects)?;
            visit_expr(high, scope, depth + 1, nodes, objects)?;
            if low.data_type() != expr.data_type() || high.data_type() != expr.data_type() {
                return Err(crate::DbError::internal(
                    "stored query BETWEEN has invalid operand types",
                ));
            }
            require_query_metadata(
                data_type,
                *nullable,
                &DataType::Boolean,
                expr.nullable() || low.nullable() || high.nullable(),
            )?;
        }
        StoredQueryExpr::Like {
            expr,
            pattern,
            data_type,
            nullable,
            ..
        } => {
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            visit_expr(pattern, scope, depth + 1, nodes, objects)?;
            if expr.data_type() != &DataType::Text || pattern.data_type() != &DataType::Text {
                return Err(crate::DbError::internal(
                    "stored query LIKE has invalid operand types",
                ));
            }
            require_query_metadata(
                data_type,
                *nullable,
                &DataType::Boolean,
                expr.nullable() || pattern.nullable(),
            )?;
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            flow_sensitive_nullable,
            data_type,
            nullable,
        } => {
            check_list(when_clauses.len())?;
            if when_clauses.is_empty() && !flow_sensitive_nullable {
                return Err(crate::DbError::internal(
                    "stored query CASE has no WHEN clauses",
                ));
            }
            if let Some(expr) = operand {
                visit_expr(expr, scope, depth + 1, nodes, objects)?;
            }
            for (when, then) in when_clauses {
                visit_expr(when, scope, depth + 1, nodes, objects)?;
                visit_expr(then, scope, depth + 1, nodes, objects)?;
                let when_matches = operand.as_ref().map_or_else(
                    || when.data_type() == &DataType::Boolean,
                    |operand| when.data_type() == operand.data_type(),
                );
                if !when_matches || then.data_type() != data_type {
                    return Err(crate::DbError::internal(
                        "stored query CASE has invalid branch types",
                    ));
                }
            }
            if let Some(expr) = else_clause {
                visit_expr(expr, scope, depth + 1, nodes, objects)?;
                if expr.data_type() != data_type {
                    return Err(crate::DbError::internal(
                        "stored query CASE has invalid ELSE type",
                    ));
                }
            }
            let branch_nullable = else_clause.is_none()
                || when_clauses.iter().any(|(_, then)| then.nullable())
                || else_clause
                    .as_deref()
                    .is_some_and(StoredQueryExpr::nullable);
            let expected_nullable = if *flow_sensitive_nullable {
                coalesce_case_nullable(operand.as_deref(), when_clauses, else_clause.as_deref())
                    .ok_or_else(|| {
                        crate::DbError::internal(
                            "stored query CASE has invalid flow-sensitive nullability metadata",
                        )
                    })?
            } else {
                branch_nullable
            };
            if *nullable != expected_nullable {
                return Err(crate::DbError::internal(
                    "stored query CASE has inconsistent nullability metadata",
                ));
            }
        }
        StoredQueryExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => {
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            if pg_type.data_type() != *data_type || *nullable != expr.nullable() {
                return Err(crate::DbError::internal(
                    "stored query cast has invalid result metadata",
                ));
            }
        }
        StoredQueryExpr::ScalarSubquery {
            query,
            data_type,
            nullable,
        } => {
            if query.output_schema().first().is_none_or(|output| {
                query.output_schema().len() != 1 || output.data_type != *data_type
            }) || !nullable
            {
                return Err(crate::DbError::internal(
                    "stored scalar subquery has invalid output metadata",
                ));
            }
            visit_query(query, Some(scope), depth + 1, nodes, objects)?
        }
        StoredQueryExpr::Exists {
            query,
            data_type,
            nullable,
            ..
        } => {
            require_query_metadata(data_type, *nullable, &DataType::Boolean, false)?;
            visit_query(query, Some(scope), depth + 1, nodes, objects)?
        }
        StoredQueryExpr::InSubquery {
            expr,
            query,
            data_type,
            nullable,
            ..
        } => {
            visit_expr(expr, scope, depth + 1, nodes, objects)?;
            if query.output_schema().first().is_none_or(|output| {
                query.output_schema().len() != 1 || output.data_type != *expr.data_type()
            }) || data_type != &DataType::Boolean
                || !nullable
            {
                return Err(crate::DbError::internal(
                    "stored IN subquery has invalid output metadata",
                ));
            }
            visit_query(query, Some(scope), depth + 1, nodes, objects)?;
        }
    }
    Ok(())
}

fn require_query_metadata(
    actual_type: &DataType,
    actual_nullable: bool,
    expected_type: &DataType,
    expected_nullable: bool,
) -> crate::Result<()> {
    if actual_type != expected_type || actual_nullable != expected_nullable {
        return Err(crate::DbError::internal(
            "stored query expression has inconsistent type metadata",
        ));
    }
    Ok(())
}

fn coalesce_case_nullable(
    operand: Option<&StoredQueryExpr>,
    when_clauses: &[(StoredQueryExpr, StoredQueryExpr)],
    else_clause: Option<&StoredQueryExpr>,
) -> Option<bool> {
    if operand.is_some() {
        return None;
    }
    let else_clause = else_clause?;
    for (when, then) in when_clauses {
        let StoredQueryExpr::IsNotNull { expr, .. } = when else {
            return None;
        };
        if expr.as_ref() != then {
            return None;
        }
    }
    Some(when_clauses.iter().all(|(_, then)| then.nullable()) && else_clause.nullable())
}

fn query_binary_result_type(
    left: &DataType,
    op: StoredQueryBinOp,
    right: &DataType,
) -> Option<DataType> {
    crate::stored_expression::binary_result_type(left, query_stored_binop(op), right)
}

fn query_numeric_family(data_type: &DataType) -> Option<u8> {
    crate::stored_expression::numeric_family(data_type)
}

fn query_is_comparison(op: StoredQueryBinOp) -> bool {
    crate::stored_expression::is_comparison(query_stored_binop(op))
}

fn query_array_shape_matches(dimensions: &[u32], element_count: usize) -> crate::Result<bool> {
    crate::stored_expression::array_shape_matches(dimensions, element_count)
}

fn query_stored_binop(op: StoredQueryBinOp) -> crate::StoredBinOp {
    match op {
        StoredQueryBinOp::Add => crate::StoredBinOp::Add,
        StoredQueryBinOp::Sub => crate::StoredBinOp::Sub,
        StoredQueryBinOp::Mul => crate::StoredBinOp::Mul,
        StoredQueryBinOp::Div => crate::StoredBinOp::Div,
        StoredQueryBinOp::Mod => crate::StoredBinOp::Mod,
        StoredQueryBinOp::Eq => crate::StoredBinOp::Eq,
        StoredQueryBinOp::Neq => crate::StoredBinOp::Neq,
        StoredQueryBinOp::Lt => crate::StoredBinOp::Lt,
        StoredQueryBinOp::LtEq => crate::StoredBinOp::LtEq,
        StoredQueryBinOp::Gt => crate::StoredBinOp::Gt,
        StoredQueryBinOp::GtEq => crate::StoredBinOp::GtEq,
        StoredQueryBinOp::And => crate::StoredBinOp::And,
        StoredQueryBinOp::Or => crate::StoredBinOp::Or,
        StoredQueryBinOp::Concat => crate::StoredBinOp::Concat,
        StoredQueryBinOp::IsDistinctFrom => crate::StoredBinOp::IsDistinctFrom,
        StoredQueryBinOp::IsNotDistinctFrom => crate::StoredBinOp::IsNotDistinctFrom,
    }
}

fn validate_query_aggregate(
    function: FunctionId,
    arg: Option<&StoredQueryExpr>,
    distinct: bool,
    data_type: &DataType,
    nullable: bool,
) -> crate::Result<()> {
    let expected = match function {
        2_147 if arg.is_none() && distinct => None,
        2_147 => Some((DataType::Integer, false)),
        2_107 | 2_100 => arg.and_then(|arg| {
            let result = match arg.data_type() {
                DataType::Integer => DataType::Integer,
                DataType::Double => DataType::Double,
                DataType::Real => DataType::Real,
                DataType::Numeric { .. } => DataType::Numeric {
                    precision: None,
                    scale: 0,
                },
                _ => return None,
            };
            Some((result, true))
        }),
        2_131 | 2_115 => arg.map(|arg| (arg.data_type().clone(), true)),
        2_713 | 2_712 | 2_644 | 2_643
            if arg.is_some_and(|arg| query_numeric_family(arg.data_type()).is_some()) =>
        {
            Some((DataType::Double, true))
        }
        2_517 | 2_518 if arg.is_some_and(|arg| arg.data_type() == &DataType::Boolean) => {
            Some((DataType::Boolean, true))
        }
        2_335 => arg.and_then(|arg| {
            if matches!(arg.data_type(), DataType::Array(_)) {
                return None;
            }
            crate::ArrayType::new(arg.data_type().clone())
                .ok()
                .map(|array| (DataType::Array(array), true))
        }),
        3_538
            if matches!(
                arg.map(StoredQueryExpr::data_type),
                Some(DataType::Array(array)) if array.element_type() == &DataType::Text
            ) =>
        {
            Some((DataType::Text, true))
        }
        _ => None,
    };
    let Some((expected_type, expected_nullable)) = expected else {
        return Err(crate::DbError::internal(format!(
            "stored query aggregate function {function} has invalid arguments"
        )));
    };
    require_query_metadata(data_type, nullable, &expected_type, expected_nullable)
}

fn collect_query_references(
    query: &StoredQueryV1,
    parent: Option<&BTreeMap<BindingId, StoredRangeTarget>>,
    column_visitor: &mut dyn FnMut(TableId, ColumnObjectId, &DataType, bool, bool),
    system_visitor: &mut dyn FnMut(i64, &[StoredRangeColumn]),
) -> crate::Result<()> {
    let mut ranges = BTreeMap::new();
    match &query.body {
        StoredQueryBody::Select(select) => {
            if let Some(from) = &select.from {
                collect_from_references(from, &mut ranges, column_visitor, system_visitor)?;
            }
            if let Some(StoredDistinct::On(exprs)) = &select.distinct {
                for expr in exprs {
                    collect_expr_references(expr, &ranges, column_visitor, system_visitor)?;
                }
            }
            for item in &select.columns {
                collect_expr_references(&item.expr, &ranges, column_visitor, system_visitor)?;
            }
            if let Some(expr) = &select.filter {
                collect_expr_references(expr, &ranges, column_visitor, system_visitor)?;
            }
            for expr in &select.group_by {
                collect_expr_references(expr, &ranges, column_visitor, system_visitor)?;
            }
            if let Some(expr) = &select.having {
                collect_expr_references(expr, &ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryBody::Values(values) => {
            for expr in values.rows.iter().flatten() {
                collect_expr_references(expr, &ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryBody::SetOp(set_op) => {
            collect_query_references(&set_op.left, parent, column_visitor, system_visitor)?;
            collect_query_references(&set_op.right, parent, column_visitor, system_visitor)?;
        }
    }
    for item in &query.order_by {
        collect_expr_references(&item.expr, &ranges, column_visitor, system_visitor)?;
    }
    let correlation_ranges = parent.unwrap_or(&ranges);
    for column in &query.correlations {
        collect_expr_references(
            &column.outer,
            correlation_ranges,
            column_visitor,
            system_visitor,
        )?;
    }
    Ok(())
}

fn collect_from_references(
    from: &StoredFrom,
    ranges: &mut BTreeMap<BindingId, StoredRangeTarget>,
    column_visitor: &mut dyn FnMut(TableId, ColumnObjectId, &DataType, bool, bool),
    system_visitor: &mut dyn FnMut(i64, &[StoredRangeColumn]),
) -> crate::Result<()> {
    match from {
        StoredFrom::Table { table, range, .. } => {
            insert_collected_range(
                ranges,
                *range,
                StoredRangeTarget::Catalog {
                    table: *table,
                    null_extended: false,
                },
            )?;
        }
        StoredFrom::System {
            relation_oid,
            range,
            schema,
            ..
        } => {
            system_visitor(*relation_oid, schema);
            insert_collected_range(ranges, *range, StoredRangeTarget::Position(schema.clone()))?;
        }
        StoredFrom::Derived {
            query,
            range,
            schema,
            ..
        } => {
            collect_query_references(query, Some(ranges), column_visitor, system_visitor)?;
            insert_collected_range(ranges, *range, StoredRangeTarget::Position(schema.clone()))?;
        }
        StoredFrom::TableFunction {
            args,
            range,
            schema,
            ..
        } => {
            for expr in args {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
            insert_collected_range(ranges, *range, StoredRangeTarget::Position(schema.clone()))?;
        }
        StoredFrom::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let before = ranges.keys().copied().collect::<BTreeSet<_>>();
            collect_from_references(left, ranges, column_visitor, system_visitor)?;
            let after_left = ranges.keys().copied().collect::<BTreeSet<_>>();
            collect_from_references(right, ranges, column_visitor, system_visitor)?;
            if let Some(expr) = condition {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
            let after_right = ranges.keys().copied().collect::<BTreeSet<_>>();
            if matches!(join_type, StoredJoinType::Right | StoredJoinType::Full) {
                mark_collected_ranges_null_extended(
                    ranges,
                    after_left.difference(&before).copied(),
                )?;
            }
            if matches!(join_type, StoredJoinType::Left | StoredJoinType::Full) {
                mark_collected_ranges_null_extended(
                    ranges,
                    after_right.difference(&after_left).copied(),
                )?;
            }
        }
    }
    Ok(())
}

fn insert_collected_range(
    ranges: &mut BTreeMap<BindingId, StoredRangeTarget>,
    range: BindingId,
    target: StoredRangeTarget,
) -> crate::Result<()> {
    if ranges.insert(range, target).is_some() {
        return Err(crate::DbError::internal(
            "stored query contains duplicate range id",
        ));
    }
    Ok(())
}

fn mark_collected_ranges_null_extended(
    ranges: &mut BTreeMap<BindingId, StoredRangeTarget>,
    ids: impl Iterator<Item = BindingId>,
) -> crate::Result<()> {
    for id in ids {
        match ranges.get_mut(&id) {
            Some(StoredRangeTarget::Catalog { null_extended, .. }) => {
                *null_extended = true;
            }
            Some(StoredRangeTarget::Position(columns)) => {
                for column in columns {
                    column.nullable = true;
                }
            }
            None => {
                return Err(crate::DbError::internal(
                    "stored query join references unknown range",
                ));
            }
        }
    }
    Ok(())
}

fn collect_expr_references(
    expr: &StoredQueryExpr,
    ranges: &BTreeMap<BindingId, StoredRangeTarget>,
    column_visitor: &mut dyn FnMut(TableId, ColumnObjectId, &DataType, bool, bool),
    system_visitor: &mut dyn FnMut(i64, &[StoredRangeColumn]),
) -> crate::Result<()> {
    match expr {
        StoredQueryExpr::InputRef {
            range,
            column: StoredColumnReference::Catalog(column),
            data_type,
            nullable,
        } => {
            let Some(StoredRangeTarget::Catalog {
                table,
                null_extended,
            }) = ranges.get(range)
            else {
                return Err(crate::DbError::internal(
                    "stored query catalog column has invalid range",
                ));
            };
            column_visitor(*table, *column, data_type, *nullable, *null_extended);
        }
        StoredQueryExpr::InputRef { .. }
        | StoredQueryExpr::Literal { .. }
        | StoredQueryExpr::LocalRef { .. }
        | StoredQueryExpr::OuterRef { .. }
        | StoredQueryExpr::Nextval { .. }
        | StoredQueryExpr::Currval { .. } => {}
        StoredQueryExpr::Binary { left, right, .. }
        | StoredQueryExpr::Any {
            left, array: right, ..
        } => {
            collect_expr_references(left, ranges, column_visitor, system_visitor)?;
            collect_expr_references(right, ranges, column_visitor, system_visitor)?;
        }
        StoredQueryExpr::Unary { expr, .. }
        | StoredQueryExpr::IsNull { expr, .. }
        | StoredQueryExpr::IsNotNull { expr, .. }
        | StoredQueryExpr::Cast { expr, .. } => {
            collect_expr_references(expr, ranges, column_visitor, system_visitor)?
        }
        StoredQueryExpr::Function { args, .. } => {
            for expr in args {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::Array { elements, .. } => {
            for expr in elements {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            collect_expr_references(array, ranges, column_visitor, system_visitor)?;
            for expr in subscripts {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::Setval {
            value, is_called, ..
        } => {
            collect_expr_references(value, ranges, column_visitor, system_visitor)?;
            if let Some(expr) = is_called {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::Aggregate { arg, .. } => {
            if let Some(expr) = arg {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::InList { expr, list, .. } => {
            collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            for item in list {
                collect_expr_references(item, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            collect_expr_references(low, ranges, column_visitor, system_visitor)?;
            collect_expr_references(high, ranges, column_visitor, system_visitor)?;
        }
        StoredQueryExpr::Like { expr, pattern, .. } => {
            collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            collect_expr_references(pattern, ranges, column_visitor, system_visitor)?;
        }
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(expr) = operand {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
            for (when, then) in when_clauses {
                collect_expr_references(when, ranges, column_visitor, system_visitor)?;
                collect_expr_references(then, ranges, column_visitor, system_visitor)?;
            }
            if let Some(expr) = else_clause {
                collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            }
        }
        StoredQueryExpr::ScalarSubquery { query, .. } | StoredQueryExpr::Exists { query, .. } => {
            collect_query_references(query, Some(ranges), column_visitor, system_visitor)?
        }
        StoredQueryExpr::InSubquery { expr, query, .. } => {
            collect_expr_references(expr, ranges, column_visitor, system_visitor)?;
            collect_query_references(query, Some(ranges), column_visitor, system_visitor)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn constant_query() -> StoredQueryV1 {
        StoredQueryV1 {
            version: STORED_QUERY_VERSION,
            body: StoredQueryBody::Select(Box::new(StoredSelect {
                distinct: None,
                columns: vec![StoredSelectItem {
                    expr: StoredQueryExpr::Literal {
                        value: Value::Integer(1),
                        data_type: DataType::Integer,
                        nullable: false,
                    },
                    alias: "value".to_string(),
                }],
                from: None,
                filter: None,
                group_by: Vec::new(),
                having: None,
                output_schema: vec![StoredQueryColumn {
                    name: "value".to_string(),
                    data_type: DataType::Integer,
                    pg_type: PgType::Int8,
                }],
            })),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            row_lock: None,
            correlations: Vec::new(),
        }
    }

    #[test]
    fn stored_query_rejects_unknown_version_and_function_category() {
        let mut query = constant_query();
        query.version += 1;
        assert!(validate_stored_query_shape(&query).is_err());

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::Function {
            function: 999_999,
            args: Vec::new(),
            data_type: DataType::Integer,
            pg_type: Some(PgType::Int8),
            nullable: false,
        };
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_rejects_out_of_range_positional_column() {
        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.from = Some(StoredFrom::Derived {
            query: Box::new(constant_query()),
            range: 0,
            alias: "d".to_string(),
            schema: vec![StoredRangeColumn {
                name: "value".to_string(),
                data_type: DataType::Integer,
                pg_type: PgType::Int8,
                nullable: false,
            }],
            lateral: false,
        });
        select.columns[0].expr = StoredQueryExpr::InputRef {
            range: 0,
            column: StoredColumnReference::Position(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_decoder_rejects_oversized_node_list() {
        let mut query = constant_query();
        query.order_by = (0..=MAX_STORED_QUERY_LIST_ITEMS)
            .map(|_| StoredOrderBy {
                expr: StoredQueryExpr::LocalRef {
                    output: 0,
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ascending: true,
                nulls_first: None,
            })
            .collect();
        let json = serde_json::to_string(&query).unwrap();
        let error = serde_json::from_str::<StoredQueryV1>(&json).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stored query list exceeds limit")
        );
    }

    #[test]
    fn stored_query_decoder_shares_an_allocation_budget_across_lists() {
        let json = serde_json::to_string(&constant_query()).unwrap();
        let error =
            with_new_decode_budget(2, || serde_json::from_str::<StoredQueryV1>(&json)).unwrap_err();
        assert!(error.to_string().contains("decode node budget"));
    }

    #[test]
    fn stored_query_decoder_budgets_boxed_expression_edges() {
        let mut expr = StoredQueryExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        for _ in 0..3 {
            expr = StoredQueryExpr::Unary {
                op: StoredQueryUnaryOp::Neg,
                expr: Box::new(expr),
                data_type: DataType::Integer,
                nullable: false,
            };
        }
        let json = serde_json::to_string(&expr).unwrap();
        let error = with_new_decode_budget(2, || serde_json::from_str::<StoredQueryExpr>(&json))
            .unwrap_err();
        assert!(error.to_string().contains("decode node budget"));
    }

    #[test]
    fn stored_query_accepts_projection_wider_than_expression_list_limit() {
        let width = MAX_STORED_QUERY_LIST_ITEMS + 1;
        let literal = StoredQueryExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        let columns = (0..width)
            .map(|index| StoredSelectItem {
                expr: literal.clone(),
                alias: format!("column{index}"),
            })
            .collect::<Vec<_>>();
        let output_schema = columns
            .iter()
            .map(|item| StoredQueryColumn {
                name: item.alias.clone(),
                data_type: DataType::Integer,
                pg_type: PgType::Int8,
            })
            .collect();
        let query = StoredQueryV1 {
            version: STORED_QUERY_VERSION,
            body: StoredQueryBody::Select(Box::new(StoredSelect {
                distinct: None,
                columns,
                from: None,
                filter: None,
                group_by: Vec::new(),
                having: None,
                output_schema,
            })),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            row_lock: None,
            correlations: Vec::new(),
        };

        validate_stored_query_shape(&query).unwrap();
        let json = serde_json::to_string(&query).unwrap();
        let decoded: StoredQueryV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.output_schema().len(), width);
    }

    #[test]
    fn stored_query_rejects_forged_expression_and_output_references() {
        let mut query = constant_query();
        query.order_by.push(StoredOrderBy {
            expr: StoredQueryExpr::LocalRef {
                output: 1,
                data_type: DataType::Integer,
                nullable: false,
            },
            ascending: true,
            nulls_first: None,
        });
        assert!(validate_stored_query_shape(&query).is_err());

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Boolean,
            nullable: false,
        };
        select.output_schema[0].data_type = DataType::Boolean;
        select.output_schema[0].pg_type = PgType::Bool;
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_decoder_rejects_oversized_values_row() {
        let literal = StoredQueryExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        let query = StoredQueryV1 {
            version: STORED_QUERY_VERSION,
            body: StoredQueryBody::Values(StoredValues {
                rows: vec![vec![literal; MAX_STORED_QUERY_COLUMNS + 1]],
                output_schema: vec![StoredQueryColumn {
                    name: "column1".to_string(),
                    data_type: DataType::Integer,
                    pg_type: PgType::Int8,
                }],
            }),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            row_lock: None,
            correlations: Vec::new(),
        };
        let json = serde_json::to_string(&query).unwrap();
        let error = serde_json::from_str::<StoredQueryV1>(&json).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stored query list exceeds limit")
        );
    }

    #[test]
    fn stored_query_rejects_forward_lateral_correlation() {
        let mut inner = constant_query();
        inner.correlations.push(StoredCorrelatedColumn {
            outer: StoredQueryExpr::InputRef {
                range: 2,
                column: StoredColumnReference::Catalog(1),
                data_type: DataType::Integer,
                nullable: false,
            },
            data_type: DataType::Integer,
            nullable: false,
        });

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.from = Some(StoredFrom::Join {
            left: Box::new(StoredFrom::Derived {
                query: Box::new(inner),
                range: 1,
                alias: "left_input".to_string(),
                schema: vec![StoredRangeColumn {
                    name: "value".to_string(),
                    data_type: DataType::Integer,
                    pg_type: PgType::Int8,
                    nullable: false,
                }],
                lateral: true,
            }),
            right: Box::new(StoredFrom::Table {
                table: 1,
                range: 2,
                alias: Some("right_input".to_string()),
            }),
            join_type: StoredJoinType::Cross,
            condition: None,
        });
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_rejects_inconsistent_derived_and_join_metadata() {
        let derived = StoredFrom::Derived {
            query: Box::new(constant_query()),
            range: 1,
            alias: "derived_input".to_string(),
            schema: vec![StoredRangeColumn {
                name: "value".to_string(),
                data_type: DataType::Integer,
                pg_type: PgType::Int8,
                nullable: false,
            }],
            lateral: false,
        };

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        let mut mismatched = derived.clone();
        let StoredFrom::Derived { schema, .. } = &mut mismatched else {
            panic!("test input is derived")
        };
        schema[0].nullable = true;
        select.from = Some(mismatched);
        assert!(validate_stored_query_shape(&query).is_err());

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        let mut right = derived.clone();
        let StoredFrom::Derived { range, .. } = &mut right else {
            panic!("test input is derived")
        };
        *range = 2;
        select.from = Some(StoredFrom::Join {
            left: Box::new(derived.clone()),
            right: Box::new(right),
            join_type: StoredJoinType::Cross,
            condition: Some(StoredQueryExpr::Literal {
                value: Value::Boolean(true),
                data_type: DataType::Boolean,
                nullable: false,
            }),
        });
        assert!(validate_stored_query_shape(&query).is_err());

        let mut right = derived.clone();
        let StoredFrom::Derived { range, .. } = &mut right else {
            panic!("test input is derived")
        };
        *range = 2;
        let left_ref = StoredQueryExpr::InputRef {
            range: 1,
            column: StoredColumnReference::Position(0),
            data_type: DataType::Integer,
            nullable: false,
        };
        let right_ref = StoredQueryExpr::InputRef {
            range: 2,
            column: StoredColumnReference::Position(0),
            data_type: DataType::Integer,
            nullable: false,
        };
        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.from = Some(StoredFrom::Join {
            left: Box::new(derived),
            right: Box::new(right),
            join_type: StoredJoinType::Left,
            condition: Some(StoredQueryExpr::Binary {
                op: StoredQueryBinOp::Eq,
                left: Box::new(left_ref),
                right: Box::new(right_ref),
                data_type: DataType::Boolean,
                nullable: false,
            }),
        });
        select.columns[0].expr = StoredQueryExpr::InputRef {
            range: 2,
            column: StoredColumnReference::Position(0),
            data_type: DataType::Integer,
            nullable: true,
        };
        assert!(validate_stored_query_shape(&query).is_ok());
    }

    #[test]
    fn stored_query_in_subquery_validates_result_nullability() {
        let mut non_nullable_query = constant_query();
        let StoredQueryBody::Select(select) = &mut non_nullable_query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::InSubquery {
            expr: Box::new(StoredQueryExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            }),
            query: Box::new(constant_query()),
            negated: false,
            data_type: DataType::Boolean,
            nullable: true,
        };
        select.output_schema[0].data_type = DataType::Boolean;
        select.output_schema[0].pg_type = PgType::Bool;
        assert!(validate_stored_query_shape(&non_nullable_query).is_ok());

        let mut nullable_query = constant_query();
        let StoredQueryBody::Select(select) = &mut nullable_query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::Literal {
            value: Value::Null,
            data_type: DataType::Integer,
            nullable: true,
        };

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::InSubquery {
            expr: Box::new(StoredQueryExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            }),
            query: Box::new(nullable_query),
            negated: false,
            data_type: DataType::Boolean,
            nullable: false,
        };
        select.output_schema[0].data_type = DataType::Boolean;
        select.output_schema[0].pg_type = PgType::Bool;
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_rejects_expressions_outside_binder_contexts() {
        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::LocalRef {
            output: 0,
            data_type: DataType::Integer,
            nullable: false,
        };
        assert!(validate_stored_query_shape(&query).is_err());

        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.filter = Some(StoredQueryExpr::Aggregate {
            function: 2_517,
            arg: Some(Box::new(StoredQueryExpr::Literal {
                value: Value::Boolean(true),
                data_type: DataType::Boolean,
                nullable: false,
            })),
            distinct: false,
            data_type: DataType::Boolean,
            nullable: true,
        });
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_rejects_forged_grouping_and_distinct_semantics() {
        let input = StoredQueryExpr::InputRef {
            range: 0,
            column: StoredColumnReference::Catalog(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        let mut ungrouped = constant_query();
        let StoredQueryBody::Select(select) = &mut ungrouped.body else {
            panic!("test query is SELECT")
        };
        select.from = Some(StoredFrom::Table {
            table: 1,
            range: 0,
            alias: None,
        });
        select.columns[0].expr = input;
        select.columns.push(StoredSelectItem {
            expr: StoredQueryExpr::Aggregate {
                function: 2_147,
                arg: None,
                distinct: false,
                data_type: DataType::Integer,
                nullable: false,
            },
            alias: "count".to_string(),
        });
        select.output_schema.push(StoredQueryColumn {
            name: "count".to_string(),
            data_type: DataType::Integer,
            pg_type: PgType::Int8,
        });
        assert!(validate_stored_query_shape(&ungrouped).is_err());

        let first = StoredQueryExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        let second = StoredQueryExpr::Literal {
            value: Value::Integer(2),
            data_type: DataType::Integer,
            nullable: false,
        };
        let mut distinct = constant_query();
        let StoredQueryBody::Select(select) = &mut distinct.body else {
            panic!("test query is SELECT")
        };
        select.distinct = Some(StoredDistinct::All);
        distinct.order_by.push(StoredOrderBy {
            expr: second.clone(),
            ascending: true,
            nulls_first: None,
        });
        assert!(validate_stored_query_shape(&distinct).is_err());

        let mut distinct_on = constant_query();
        let StoredQueryBody::Select(select) = &mut distinct_on.body else {
            panic!("test query is SELECT")
        };
        select.distinct = Some(StoredDistinct::On(vec![first]));
        distinct_on.order_by.push(StoredOrderBy {
            expr: second,
            ascending: true,
            nulls_first: None,
        });
        assert!(validate_stored_query_shape(&distinct_on).is_err());
    }

    #[test]
    fn select_semantics_treats_distinct_on_aggregate_as_aggregate_context() {
        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::InputRef {
            range: 0,
            column: StoredColumnReference::Catalog(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        select.distinct = Some(StoredDistinct::On(vec![StoredQueryExpr::Aggregate {
            function: 2_147,
            arg: None,
            distinct: false,
            data_type: DataType::Integer,
            nullable: false,
        }]));

        assert!(validate_select_semantics(select, &[]).is_err());
    }

    #[test]
    fn stored_query_rejects_invalid_row_lock_shape() {
        let mut query = constant_query();
        query.row_lock = Some(StoredRowLock {
            table: 1,
            mode: StoredTupleLockMode::Update,
            wait_policy: StoredTupleLockWaitPolicy::Block,
        });
        assert!(validate_stored_query_shape(&query).is_err());
    }

    #[test]
    fn stored_query_rejects_join_nesting_at_the_depth_limit() {
        let mut from = StoredFrom::Table {
            table: 1,
            range: 0,
            alias: None,
        };
        for range in 1..=MAX_STORED_QUERY_DEPTH {
            from = StoredFrom::Join {
                left: Box::new(from),
                right: Box::new(StoredFrom::Table {
                    table: u32::try_from(range + 1).unwrap(),
                    range: u32::try_from(range).unwrap(),
                    alias: None,
                }),
                join_type: StoredJoinType::Cross,
                condition: None,
            };
        }
        let mut query = constant_query();
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.from = Some(from);
        let error = validate_stored_query_shape(&query).unwrap_err();
        assert_eq!(error.code, crate::SqlState::ProgramLimitExceeded);
    }

    #[test]
    fn stored_query_validates_exact_output_postgresql_types() {
        let mut computed = constant_query();
        let StoredQueryBody::Select(select) = &mut computed.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::Literal {
            value: Value::Text("value".to_string()),
            data_type: DataType::Text,
            nullable: false,
        };
        select.output_schema[0].data_type = DataType::Text;
        select.output_schema[0].pg_type = PgType::Varchar(Some(8));
        assert!(
            computed
                .validate_output_pg_types(&mut |_, _| Ok(PgType::Text))
                .is_err()
        );

        let mut catalog_column = constant_query();
        let StoredQueryBody::Select(select) = &mut catalog_column.body else {
            panic!("test query is SELECT")
        };
        select.from = Some(StoredFrom::Table {
            table: 7,
            range: 3,
            alias: None,
        });
        select.columns[0].expr = StoredQueryExpr::InputRef {
            range: 3,
            column: StoredColumnReference::Catalog(11),
            data_type: DataType::Integer,
            nullable: false,
        };
        select.output_schema[0].pg_type = PgType::Int4;
        assert!(
            catalog_column
                .validate_output_pg_types(&mut |table, column| {
                    assert_eq!((table, column), (7, 11));
                    Ok(PgType::Int2)
                })
                .is_err()
        );
    }

    #[test]
    fn stored_query_live_limits_use_program_limit_sqlstate() {
        let mut query = constant_query();
        query.order_by = (0..=MAX_STORED_QUERY_LIST_ITEMS)
            .map(|_| StoredOrderBy {
                expr: StoredQueryExpr::Literal {
                    value: Value::Integer(1),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ascending: true,
                nulls_first: None,
            })
            .collect();
        let error = validate_stored_query_shape(&query).unwrap_err();
        assert_eq!(error.code, crate::SqlState::ProgramLimitExceeded);
    }

    #[test]
    fn stored_query_case_nullability_uses_declared_rule() {
        let integer = StoredQueryExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Integer,
            nullable: false,
        };
        let mut query = constant_query();
        {
            let StoredQueryBody::Select(select) = &mut query.body else {
                panic!("test query is SELECT")
            };
            select.columns[0].expr = StoredQueryExpr::Case {
                operand: None,
                when_clauses: vec![(
                    StoredQueryExpr::Literal {
                        value: Value::Boolean(true),
                        data_type: DataType::Boolean,
                        nullable: false,
                    },
                    integer.clone(),
                )],
                else_clause: Some(Box::new(integer)),
                flow_sensitive_nullable: false,
                data_type: DataType::Integer,
                nullable: true,
            };
        }
        assert!(validate_stored_query_shape(&query).is_err());

        let nullable_arg = StoredQueryExpr::Literal {
            value: Value::Null,
            data_type: DataType::Integer,
            nullable: true,
        };
        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::Case {
            operand: None,
            when_clauses: vec![(
                StoredQueryExpr::IsNotNull {
                    expr: Box::new(nullable_arg.clone()),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
                nullable_arg,
            )],
            else_clause: Some(Box::new(StoredQueryExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            })),
            flow_sensitive_nullable: true,
            data_type: DataType::Integer,
            nullable: false,
        };
        assert!(validate_stored_query_shape(&query).is_ok());

        let StoredQueryBody::Select(select) = &mut query.body else {
            panic!("test query is SELECT")
        };
        select.columns[0].expr = StoredQueryExpr::Case {
            operand: None,
            when_clauses: Vec::new(),
            else_clause: Some(Box::new(StoredQueryExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            })),
            flow_sensitive_nullable: true,
            data_type: DataType::Integer,
            nullable: false,
        };
        assert!(validate_stored_query_shape(&query).is_ok());
    }
}
