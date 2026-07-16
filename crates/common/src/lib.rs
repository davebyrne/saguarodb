#![cfg_attr(
    not(test),
    deny(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

pub mod array;
pub mod bytea;
pub mod cancel;
pub mod catalog_change;
pub mod checked_bytes;
pub mod concurrency;
pub mod context;
pub mod copy;
pub mod datetime;
mod durable;
pub mod error;
pub mod float;
pub mod flush;
pub mod functions;
pub mod ids;
pub mod interval;
pub mod locking;
pub mod mvcc;
pub mod numeric;
pub mod pg_type;
pub mod row;
pub mod schema;
pub mod statistics;
pub mod stored_expression;
pub mod stored_query;
pub mod uuid;
pub mod value;

/// PostgreSQL compatibility version reported to clients through
/// `server_version`, startup `ParameterStatus`, and `version()`.
pub const POSTGRES_COMPAT_VERSION: &str = "16.0";

pub use array::{
    ArrayDimension, MAX_ARRAY_DIMENSIONS, MAX_ARRAY_ELEMENTS, SqlArray,
    format_array_text_structure, parse_array_text_structure, value_matches_type,
};
pub use cancel::{CancelReason, QueryCancel};
pub use catalog_change::{
    CATALOG_CHANGE_SET_VERSION, CatalogAllocatorHighWater, CatalogChangeSet, CatalogMutation,
    CatalogObject, CatalogObjectId, DependencyEdge, DependencyType, MAX_CATALOG_CHANGE_MUTATIONS,
};
pub use checked_bytes::{CheckedSliceReader, SliceReadError, SliceReadErrorKind};
pub use concurrency::{
    CheckpointGuard, ConcurrencyController, RwLockConcurrencyController, WriteGuard,
};
pub use context::{
    CatalogIntrospectionProvider, ConflictWaiter, GucSetting, RuntimeValueSet, RuntimeValueSetId,
    RuntimeValueSetRegistry, SequenceManager, SessionActivityRow, SessionInfo,
    SessionSequenceState, SessionState, SsiTracker, StatementContext, SystemStateProvider,
    no_catalog_introspection, no_system_state,
};
pub use copy::{CopyDirection, CopyFormat, CopyOptions};
#[doc(hidden)]
pub use durable::deserialize_bounded_vec_named;
pub use error::{DbError, ErrorKind, Result, SqlState};
pub use float::{OrderedF32, OrderedF64};
pub use flush::{FlushPolicy, PageFlushInfo};
pub use functions::{
    ArgType, NullHandling, PgProcCatalogEntry, ScalarFunction, format_type_oid,
    lookup_scalar_function, lookup_scalar_function_by_id, pg_proc_catalog_entries,
    pg_proc_catalog_entry, scalar_function_arg_hint, scalar_function_arg_pg_type,
    scalar_function_id, scalar_function_id_matches, scalar_function_result_pg_type,
};
pub use ids::{
    BindingId, ColumnId, ColumnObjectId, ConstraintId, FIRST_NORMAL_XID, FIRST_USER_SCHEMA_ID,
    FROZEN_XID, FileId, FunctionId, INVALID_XID, IndexId, Lsn, PRIMARY_KEY_INDEX_ID,
    PUBLIC_SCHEMA_ID, PageNum, RowId, SchemaId, SequenceId, TableId, TxnId,
};
pub use interval::Interval;
pub use locking::{
    TupleLockAcquire, TupleLockGrantChange, TupleLockManager, TupleLockMode, TupleLockTag,
    TupleLockWaitPolicy,
};
pub use mvcc::{
    IsolationLevel, Snapshot, TxnStatus, TxnStatusView, UniqueConflict, WriteConflict,
    XMAX_ABORTED, XMAX_COMMITTED, XMIN_ABORTED, XMIN_COMMITTED, classify_unique_conflict,
    is_dead_to_all, is_visible, version_conflicts, write_conflict,
};
pub use numeric::{Decimal, RoundingStrategy};
pub use pg_type::PgType;
pub use row::{ExecRow, Key, KeyRange, Row, RowIdentity, StoredRow};
pub use schema::{
    ArrayType, ColumnDef, ColumnDefault, ColumnInfo, CompressionSetting, ConstraintKind,
    ConstraintSchema, DataType, ForeignKeyAction, ForeignKeyConstraint, INITIAL_SCHEMA_VERSION,
    IndexSchema, NamespaceSchema, ParsedColumnDef, ParsedDefault, QualifiedName, RelationKind,
    SequenceOptions, SequenceSchema, TableOptionPatch, TableSchema, ToastCompression, ToastMode,
    ToastOptionPatch, ToastOptions, TruncateCatalogUpdate, TruncateTablePlan,
    VIEW_SCHEMA_FORMAT_VERSION, ViewColumn, ViewSchema, needs_toast_relation, toast_relation_name,
    toast_schema,
};
pub use statistics::{ColumnStatistics, NDistinct, TableStatistics, value_is_finite};
pub use stored_expression::{
    MAX_STORED_EXPRESSION_DEPTH, MAX_STORED_EXPRESSION_LIST_ITEMS, MAX_STORED_EXPRESSION_NODES,
    MAX_STORED_EXPRESSION_SQL_BYTES, STORED_EXPRESSION_VERSION, StoredBinOp, StoredExpr,
    StoredExpression, StoredUnaryOp, validate_stored_expression_shape,
};
pub use stored_query::*;
pub use value::{Value, parse_bool_text};
