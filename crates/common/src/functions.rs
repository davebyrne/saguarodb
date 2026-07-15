//! Scalar function-dispatch registry.
//!
//! Each built-in scalar function is one [`ScalarFunction`] entry in the
//! [`SCALAR_FUNCTIONS`] table, pairing its bind-time signature check with its
//! run-time evaluator. The binder (`planner`) consults [`lookup_scalar_function`]
//! to validate a call and assign its result type; the executor calls the same
//! entry's `eval` to compute the value. Adding a function is a single table
//! entry — its signature and evaluation live together here rather than split
//! across the two crates.
//!
//! Sequence functions (`nextval`/`currval`/`setval`), aggregates, and the
//! NULL-folding forms `COALESCE`/`NULLIF` are *not* registered here: they have
//! their own bound representations and binding rules.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::{
    DataType, DbError, FunctionId, POSTGRES_COMPAT_VERSION, PgType, Result, SqlState,
    StatementContext, Value,
};

/// A bound argument as seen by a function's signature checker: its resolved type
/// and, when the argument is a constant, the literal value. `literal` is only
/// consulted by functions that must validate a constant at bind time (currently
/// `EXTRACT`, which checks its field name).
pub struct ArgType<'a> {
    pub data_type: DataType,
    pub literal: Option<&'a Value>,
}

/// How a scalar function treats a NULL argument.
pub enum NullHandling {
    /// A NULL argument makes the call evaluate to NULL without invoking `eval`,
    /// and the result type is nullable when any argument is. This is the rule for
    /// almost every function.
    Propagate,
    /// A NULL argument still evaluates to NULL without invoking `eval`, but the
    /// function may also return NULL for non-NULL inputs (for example, metadata
    /// lookups for a missing OID). The result type is always nullable.
    Nullable,
    /// `eval` is always invoked and owns NULL semantics. The result type is
    /// always nullable. Used for compatibility functions where only some NULL
    /// arguments propagate, such as `format_type(oid, NULL)`.
    EvaluateNullable,
    /// `eval` is always invoked (it decides how NULL is handled) and the result is
    /// never NULL, so the result type is non-nullable. Used by `CONCAT` (ignores
    /// NULL arguments) and the zero-argument system information functions.
    NeverNull,
}

/// One built-in scalar function: its canonical (lowercase) name, NULL policy,
/// bind-time signature check, and run-time evaluator.
///
/// `signature` validates arity and argument types and returns the result
/// [`DataType`]; result nullability is derived centrally from `null_handling`, so
/// a checker never has to compute it. `eval` receives the already-evaluated
/// argument values (with NULL handling applied per `null_handling`).
pub struct ScalarFunction {
    pub name: &'static str,
    pub null_handling: NullHandling,
    pub signature: fn(name: &str, args: &[ArgType]) -> Result<DataType>,
    pub eval: fn(ctx: &StatementContext, values: &[Value]) -> Result<Value>,
}

impl ScalarFunction {
    /// The result type's nullability for a call, given whether each argument is
    /// nullable. A `Propagate` function's result is nullable when any argument is;
    /// a `NeverNull` function's result is never nullable. The binder uses this so
    /// the NULL rule lives with the function definition rather than being
    /// re-derived at the call site.
    pub fn result_nullable(&self, arg_nullable: impl IntoIterator<Item = bool>) -> bool {
        match self.null_handling {
            NullHandling::Propagate => arg_nullable.into_iter().any(|nullable| nullable),
            NullHandling::Nullable | NullHandling::EvaluateNullable => true,
            NullHandling::NeverNull => false,
        }
    }
}

/// Look up a scalar function by its lowercase name. Returns `None` for names that
/// are not registered built-ins.
pub fn lookup_scalar_function(name: &str) -> Option<&'static ScalarFunction> {
    static INDEX: OnceLock<HashMap<&'static str, &'static ScalarFunction>> = OnceLock::new();
    INDEX
        .get_or_init(|| {
            SCALAR_FUNCTIONS
                .iter()
                .map(|func| (func.name, func))
                .collect()
        })
        .get(name)
        .copied()
}

/// Resolve the durable built-in identity for an already type-checked call.
pub fn scalar_function_id(
    name: &str,
    argument_types: &[DataType],
    result_type: &DataType,
) -> Option<FunctionId> {
    PG_PROC_CATALOG_ENTRIES
        .iter()
        .find(|entry| {
            entry.name == name
                && PgType::from_oid_typmod(entry.ret_oid, -1)
                    .is_some_and(|pg_type| pg_type.data_type() == *result_type)
                && catalog_entry_accepts_arguments(entry, argument_types)
        })
        .and_then(|entry| FunctionId::try_from(entry.oid).ok())
}

/// Look up a registered scalar function through its durable PostgreSQL OID.
pub fn lookup_scalar_function_by_id(
    id: FunctionId,
) -> Option<(&'static ScalarFunction, &'static PgProcCatalogEntry)> {
    let oid = i64::from(id);
    let entry = pg_proc_catalog_entry(oid)?;
    Some((lookup_scalar_function(entry.name)?, entry))
}

/// Whether a durable built-in ID exactly describes the stored call metadata.
pub fn scalar_function_id_matches(
    id: FunctionId,
    argument_types: &[DataType],
    result_type: &DataType,
    result_pg_type: Option<&PgType>,
) -> bool {
    let Some((_, entry)) = lookup_scalar_function_by_id(id) else {
        return false;
    };
    let Some(entry_result) = PgType::from_oid_typmod(entry.ret_oid, -1) else {
        return false;
    };
    entry_result.data_type() == *result_type
        && result_pg_type.is_none_or(|pg_type| *pg_type == entry_result)
        && catalog_entry_accepts_arguments(entry, argument_types)
}

fn catalog_entry_accepts_arguments(
    entry: &PgProcCatalogEntry,
    argument_types: &[DataType],
) -> bool {
    let variadic = entry.variadic_oid();
    if variadic == 0 {
        return entry.arg_oids.len() == argument_types.len()
            && entry
                .arg_oids
                .iter()
                .zip(argument_types)
                .all(|(oid, data_type)| pg_oid_has_data_type(*oid, data_type));
    }
    let Some(fixed_count) = entry.arg_oids.len().checked_sub(1) else {
        return false;
    };
    let Some(fixed_oids) = entry.arg_oids.get(..fixed_count) else {
        return false;
    };
    let Some(fixed_arguments) = argument_types.get(..fixed_count) else {
        return false;
    };
    if argument_types.len() < entry.arg_oids.len()
        || !fixed_oids
            .iter()
            .zip(fixed_arguments)
            .all(|(oid, data_type)| pg_oid_has_data_type(*oid, data_type))
    {
        return false;
    }
    argument_types
        .get(fixed_count..)
        .is_some_and(|variadic_arguments| {
            variadic_arguments
                .iter()
                .all(|data_type| pg_oid_has_data_type(variadic, data_type))
        })
}

fn pg_oid_has_data_type(oid: i64, data_type: &DataType) -> bool {
    PgType::from_oid_typmod(oid, -1).is_some_and(|pg_type| pg_type.data_type() == *data_type)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgProcCatalogEntry {
    pub oid: i64,
    pub name: &'static str,
    pub ret_oid: i64,
    pub arg_oids: &'static [i64],
}

impl PgProcCatalogEntry {
    pub fn variadic_oid(self) -> i64 {
        if self.name == "concat" {
            PG_PROC_TEXT
        } else {
            0
        }
    }
}

pub fn pg_proc_catalog_entries() -> &'static [PgProcCatalogEntry] {
    PG_PROC_CATALOG_ENTRIES
}

pub fn pg_proc_catalog_entry(oid: i64) -> Option<&'static PgProcCatalogEntry> {
    PG_PROC_CATALOG_ENTRIES
        .iter()
        .find(|entry| entry.oid == oid)
}

pub fn format_type_oid(oid: i64) -> String {
    PgType::from_oid_typmod(oid, -1)
        .map(|pg_type| pg_type.format_type_name())
        .unwrap_or_else(|| "???".to_string())
}

pub fn scalar_function_result_pg_type(
    name: &str,
    arity: usize,
    data_type: &DataType,
) -> Option<PgType> {
    let mut candidates = PG_PROC_CATALOG_ENTRIES
        .iter()
        .filter(|entry| entry.name == name && entry.arg_oids.len() == arity)
        .filter_map(|entry| PgType::from_oid_typmod(entry.ret_oid, -1))
        .filter(|pg_type| pg_type.data_type() == *data_type);
    let first = candidates.next()?;
    if candidates.all(|candidate| candidate == first) {
        Some(first)
    } else {
        None
    }
}

pub fn scalar_function_arg_pg_type(name: &str, arity: usize, index: usize) -> Option<PgType> {
    let mut candidates = PG_PROC_CATALOG_ENTRIES
        .iter()
        .filter(|entry| entry.name == name && entry.arg_oids.len() == arity)
        .filter_map(|entry| entry.arg_oids.get(index))
        .filter_map(|oid| PgType::from_oid_typmod(*oid, -1));
    let first = candidates.next()?;
    if candidates.all(|candidate| candidate == first) {
        Some(first)
    } else {
        None
    }
}

/// Expected argument type for functions whose common tool probes often pass
/// untyped NULLs or bind placeholders. The registered signature remains the
/// source of truth: this returns a hint only when a bounded search over supported
/// argument types finds exactly one type that can make the signature succeed at
/// `index`. `None` means there is no single useful type hint and ordinary
/// inference applies.
pub fn scalar_function_arg_hint(name: &str, arity: usize, index: usize) -> Option<DataType> {
    const MAX_HINT_ARITY: usize = 4;
    if index >= arity {
        return None;
    }
    let func = lookup_scalar_function(name)?;
    if arity > MAX_HINT_ARITY {
        return uniform_scalar_function_arg_hint(func, arity);
    }
    let mut accepted = hint_candidate_types()
        .iter()
        .filter(|candidate| signature_accepts_with_fixed_arg(func, arity, index, candidate))
        .cloned();
    let hint = accepted.next()?;
    if accepted.next().is_none() {
        Some(hint)
    } else {
        None
    }
}

fn uniform_scalar_function_arg_hint(func: &ScalarFunction, arity: usize) -> Option<DataType> {
    let mut accepted = hint_candidate_types()
        .iter()
        .filter(|candidate| signature_accepts_uniform_args(func, arity, candidate))
        .cloned();
    let hint = accepted.next()?;
    if accepted.next().is_none() {
        Some(hint)
    } else {
        None
    }
}

const PG_PROC_TEXT: i64 = 25;
const PG_PROC_OID: i64 = 26;
const PG_PROC_OIDVECTOR: i64 = 30;
const PG_PROC_INT4: i64 = 23;
const PG_PROC_INT8: i64 = 20;
const PG_PROC_BOOL: i64 = 16;
const PG_PROC_FLOAT8: i64 = 701;
const PG_PROC_DATE: i64 = 1082;
const PG_PROC_TIMESTAMP: i64 = 1114;
const PG_PROC_TIMESTAMPTZ: i64 = 1184;
const PG_PROC_TEXT_ARGS_1: &[i64] = &[PG_PROC_TEXT];
const PG_PROC_TEXT_ARGS_2: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_TEXT_ARGS_3: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_OIDVECTOR_ARGS_1: &[i64] = &[PG_PROC_OIDVECTOR];
const PG_PROC_FLOAT8_ARGS_1: &[i64] = &[PG_PROC_FLOAT8];
const PG_PROC_FLOAT8_ARGS_2: &[i64] = &[PG_PROC_FLOAT8, PG_PROC_FLOAT8];
const PG_PROC_INT8_ARGS_1: &[i64] = &[PG_PROC_INT8];
const PG_PROC_INT8_ARGS_2: &[i64] = &[PG_PROC_INT8, PG_PROC_INT8];
const PG_PROC_INT8_FLOAT8_ARGS: &[i64] = &[PG_PROC_INT8, PG_PROC_FLOAT8];
const PG_PROC_FLOAT8_INT8_ARGS: &[i64] = &[PG_PROC_FLOAT8, PG_PROC_INT8];
const PG_PROC_OID_ARGS_1: &[i64] = &[PG_PROC_OID];
const PG_PROC_OID_INT4_ARGS: &[i64] = &[PG_PROC_OID, PG_PROC_INT4];
const PG_PROC_OID_INT4_BOOL_ARGS: &[i64] = &[PG_PROC_OID, PG_PROC_INT4, PG_PROC_BOOL];
const PG_PROC_OID_BOOL_ARGS: &[i64] = &[PG_PROC_OID, PG_PROC_BOOL];
const PG_PROC_EXPR_ARGS: &[i64] = &[PG_PROC_TEXT, PG_PROC_OID];
const PG_PROC_EXPR_PRETTY_ARGS: &[i64] = &[PG_PROC_TEXT, PG_PROC_OID, PG_PROC_BOOL];
const PG_PROC_EXTRACT_DATE_ARGS: &[i64] = &[PG_PROC_TEXT, PG_PROC_DATE];
const PG_PROC_EXTRACT_TIMESTAMP_ARGS: &[i64] = &[PG_PROC_TEXT, PG_PROC_TIMESTAMP];
const PG_PROC_OID_TEXT_ARGS: &[i64] = &[PG_PROC_OID, PG_PROC_TEXT];
const PG_PROC_SERIAL_ARGS: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_PRIV_ARGS_2: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_PRIV_ARGS_3: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_PRIV_OID_ARGS_2: &[i64] = &[PG_PROC_OID, PG_PROC_TEXT];
const PG_PROC_PRIV_OID_ARGS_3: &[i64] = &[PG_PROC_OID, PG_PROC_OID, PG_PROC_TEXT];
const PG_PROC_COLUMN_PRIV_ARGS_3: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_COLUMN_PRIV_ARGS: &[i64] = &[PG_PROC_TEXT, PG_PROC_TEXT, PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_COLUMN_PRIV_OID_ARGS_3: &[i64] = &[PG_PROC_OID, PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_COLUMN_PRIV_OID_ARGS: &[i64] =
    &[PG_PROC_OID, PG_PROC_OID, PG_PROC_TEXT, PG_PROC_TEXT];
const PG_PROC_NO_ARGS: &[i64] = &[];

static PG_PROC_CATALOG_ENTRIES: &[PgProcCatalogEntry] = &[
    PgProcCatalogEntry {
        oid: 14_000,
        name: "upper",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_001,
        name: "lower",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_002,
        name: "trim",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_003,
        name: "length",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_071,
        name: "abs",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_INT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_072,
        name: "floor",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_073,
        name: "ceil",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_074,
        name: "ceiling",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_075,
        name: "round",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_076,
        name: "sqrt",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_077,
        name: "power",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_078,
        name: "pow",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_079,
        name: "mod",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_INT8_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_080,
        name: "extract",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_EXTRACT_DATE_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_081,
        name: "extract",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_EXTRACT_TIMESTAMP_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_004,
        name: "concat",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_005,
        name: "substring",
        ret_oid: PG_PROC_TEXT,
        arg_oids: &[PG_PROC_TEXT, PG_PROC_INT8],
    },
    PgProcCatalogEntry {
        oid: 14_006,
        name: "substring",
        ret_oid: PG_PROC_TEXT,
        arg_oids: &[PG_PROC_TEXT, PG_PROC_INT8, PG_PROC_INT8],
    },
    PgProcCatalogEntry {
        oid: 14_007,
        name: "replace",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_TEXT_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_008,
        name: "position",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_TEXT_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_009,
        name: "left",
        ret_oid: PG_PROC_TEXT,
        arg_oids: &[PG_PROC_TEXT, PG_PROC_INT8],
    },
    PgProcCatalogEntry {
        oid: 14_010,
        name: "right",
        ret_oid: PG_PROC_TEXT,
        arg_oids: &[PG_PROC_TEXT, PG_PROC_INT8],
    },
    PgProcCatalogEntry {
        oid: 14_011,
        name: "now",
        ret_oid: PG_PROC_TIMESTAMPTZ,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_012,
        name: "current_timestamp",
        ret_oid: PG_PROC_TIMESTAMPTZ,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_013,
        name: "version",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_014,
        name: "current_database",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_015,
        name: "current_catalog",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_016,
        name: "current_schema",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_017,
        name: "current_user",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_018,
        name: "session_user",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_019,
        name: "user",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_020,
        name: "pg_backend_pid",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_021,
        name: "current_setting",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_022,
        name: "format_type",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_INT4_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_023,
        name: "pg_get_indexdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_024,
        name: "pg_get_indexdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_INT4_BOOL_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_025,
        name: "pg_table_is_visible",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_026,
        name: "pg_get_expr",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_EXPR_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_027,
        name: "pg_get_expr",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_EXPR_PRETTY_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_028,
        name: "pg_get_constraintdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_029,
        name: "pg_get_constraintdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_BOOL_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_030,
        name: "pg_get_userbyid",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_031,
        name: "pg_get_serial_sequence",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_SERIAL_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_032,
        name: "to_regclass",
        ret_oid: PG_PROC_OID,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_033,
        name: "to_regtype",
        ret_oid: PG_PROC_OID,
        arg_oids: PG_PROC_TEXT_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_034,
        name: "has_table_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_035,
        name: "has_table_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_036,
        name: "has_schema_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_037,
        name: "has_schema_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_038,
        name: "has_database_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_039,
        name: "has_database_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_040,
        name: "has_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_COLUMN_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_041,
        name: "has_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_COLUMN_PRIV_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_042,
        name: "has_sequence_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_043,
        name: "has_sequence_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_044,
        name: "has_function_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_045,
        name: "has_function_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_046,
        name: "has_any_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_047,
        name: "has_any_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_048,
        name: "pg_has_role",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_049,
        name: "pg_has_role",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_050,
        name: "obj_description",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_051,
        name: "obj_description",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_TEXT_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_052,
        name: "col_description",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_INT4_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_053,
        name: "pg_relation_size",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_054,
        name: "pg_table_size",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_055,
        name: "pg_indexes_size",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_056,
        name: "pg_total_relation_size",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_057,
        name: "pg_my_temp_schema",
        ret_oid: PG_PROC_OID,
        arg_oids: PG_PROC_NO_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_058,
        name: "pg_is_other_temp_schema",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_059,
        name: "pg_get_viewdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_060,
        name: "pg_get_viewdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_BOOL_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_061,
        name: "pg_get_functiondef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_062,
        name: "pg_get_function_arguments",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_063,
        name: "pg_get_function_result",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_064,
        name: "pg_get_triggerdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_065,
        name: "pg_get_triggerdef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_BOOL_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_066,
        name: "pg_get_ruledef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_067,
        name: "pg_get_ruledef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_BOOL_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_068,
        name: "pg_get_partkeydef",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_069,
        name: "pg_function_is_visible",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_OID_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_070,
        name: "oidvectortypes",
        ret_oid: PG_PROC_TEXT,
        arg_oids: PG_PROC_OIDVECTOR_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_082,
        name: "has_table_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_083,
        name: "has_table_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_084,
        name: "has_schema_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_085,
        name: "has_schema_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_086,
        name: "has_database_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_087,
        name: "has_database_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_088,
        name: "has_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_COLUMN_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_089,
        name: "has_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_COLUMN_PRIV_OID_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_090,
        name: "has_sequence_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_091,
        name: "has_sequence_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_092,
        name: "has_function_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_093,
        name: "has_function_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_094,
        name: "has_any_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_095,
        name: "has_any_column_privilege",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_096,
        name: "pg_has_role",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_097,
        name: "pg_has_role",
        ret_oid: PG_PROC_BOOL,
        arg_oids: PG_PROC_PRIV_OID_ARGS_3,
    },
    PgProcCatalogEntry {
        oid: 14_098,
        name: "abs",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_099,
        name: "floor",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_INT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_100,
        name: "ceil",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_INT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_101,
        name: "ceiling",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_INT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_102,
        name: "round",
        ret_oid: PG_PROC_INT8,
        arg_oids: PG_PROC_INT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_103,
        name: "sqrt",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_INT8_ARGS_1,
    },
    PgProcCatalogEntry {
        oid: 14_104,
        name: "power",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_INT8_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_105,
        name: "power",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_INT8_FLOAT8_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_106,
        name: "power",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_INT8_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_107,
        name: "pow",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_INT8_ARGS_2,
    },
    PgProcCatalogEntry {
        oid: 14_108,
        name: "pow",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_INT8_FLOAT8_ARGS,
    },
    PgProcCatalogEntry {
        oid: 14_109,
        name: "pow",
        ret_oid: PG_PROC_FLOAT8,
        arg_oids: PG_PROC_FLOAT8_INT8_ARGS,
    },
];

/// The complete built-in scalar function table. Ordered by category for reading;
/// lookups go through [`lookup_scalar_function`]'s name index.
static SCALAR_FUNCTIONS: &[ScalarFunction] = &[
    // --- Text ---
    ScalarFunction {
        name: "upper",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_upper,
    },
    ScalarFunction {
        name: "lower",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_lower,
    },
    ScalarFunction {
        name: "trim",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_trim,
    },
    ScalarFunction {
        name: "length",
        null_handling: NullHandling::Propagate,
        signature: sig_length,
        eval: eval_length,
    },
    // --- Math ---
    ScalarFunction {
        name: "abs",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_abs,
    },
    ScalarFunction {
        name: "floor",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_floor,
    },
    ScalarFunction {
        name: "ceil",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_ceil,
    },
    ScalarFunction {
        name: "ceiling",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_ceil,
    },
    ScalarFunction {
        name: "round",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_round,
    },
    ScalarFunction {
        name: "sqrt",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_to_double,
        eval: eval_sqrt,
    },
    ScalarFunction {
        name: "power",
        null_handling: NullHandling::Propagate,
        signature: sig_power,
        eval: eval_power,
    },
    ScalarFunction {
        name: "pow",
        null_handling: NullHandling::Propagate,
        signature: sig_power,
        eval: eval_power,
    },
    ScalarFunction {
        name: "mod",
        null_handling: NullHandling::Propagate,
        signature: sig_mod,
        eval: eval_mod,
    },
    // --- String ---
    ScalarFunction {
        name: "replace",
        null_handling: NullHandling::Propagate,
        signature: sig_replace,
        eval: eval_replace,
    },
    ScalarFunction {
        name: "position",
        null_handling: NullHandling::Propagate,
        signature: sig_position,
        eval: eval_position,
    },
    ScalarFunction {
        name: "left",
        null_handling: NullHandling::Propagate,
        signature: sig_text_integer_to_text,
        eval: eval_left,
    },
    ScalarFunction {
        name: "right",
        null_handling: NullHandling::Propagate,
        signature: sig_text_integer_to_text,
        eval: eval_right,
    },
    ScalarFunction {
        name: "concat",
        null_handling: NullHandling::NeverNull,
        signature: sig_concat,
        eval: eval_concat,
    },
    ScalarFunction {
        name: "substring",
        null_handling: NullHandling::Propagate,
        signature: sig_substring,
        eval: eval_substring,
    },
    // --- Date/time ---
    ScalarFunction {
        name: "extract",
        null_handling: NullHandling::Propagate,
        signature: sig_extract,
        eval: eval_extract,
    },
    ScalarFunction {
        name: "current_timestamp",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_timestamptz,
        eval: eval_statement_timestamp,
    },
    ScalarFunction {
        name: "now",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_timestamptz,
        eval: eval_statement_timestamp,
    },
    // --- System information ---
    ScalarFunction {
        name: "version",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_version,
    },
    ScalarFunction {
        name: "current_database",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_database,
    },
    ScalarFunction {
        name: "current_catalog",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_database,
    },
    ScalarFunction {
        name: "current_schema",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_schema,
    },
    ScalarFunction {
        name: "current_user",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_user,
    },
    ScalarFunction {
        name: "session_user",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_user,
    },
    ScalarFunction {
        name: "user",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_user,
    },
    ScalarFunction {
        name: "pg_backend_pid",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_integer,
        eval: eval_pg_backend_pid,
    },
    ScalarFunction {
        name: "current_setting",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_current_setting,
    },
    // --- PostgreSQL catalog introspection compatibility ---
    ScalarFunction {
        name: "format_type",
        null_handling: NullHandling::EvaluateNullable,
        signature: sig_format_type,
        eval: eval_format_type,
    },
    ScalarFunction {
        name: "pg_get_indexdef",
        null_handling: NullHandling::Nullable,
        signature: sig_pg_get_indexdef,
        eval: eval_pg_get_indexdef,
    },
    ScalarFunction {
        name: "pg_table_is_visible",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_boolean,
        eval: eval_pg_table_is_visible,
    },
    ScalarFunction {
        name: "pg_get_expr",
        null_handling: NullHandling::Nullable,
        signature: sig_pg_get_expr,
        eval: eval_pg_get_expr,
    },
    ScalarFunction {
        name: "pg_get_constraintdef",
        null_handling: NullHandling::Nullable,
        signature: sig_pg_get_constraintdef,
        eval: eval_pg_get_constraintdef,
    },
    ScalarFunction {
        name: "pg_get_userbyid",
        null_handling: NullHandling::Nullable,
        signature: sig_integer_to_text,
        eval: eval_pg_get_userbyid,
    },
    ScalarFunction {
        name: "has_table_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "has_schema_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "has_database_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "has_column_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_3_or_4,
        eval: eval_true,
    },
    ScalarFunction {
        name: "has_sequence_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "has_function_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "has_any_column_privilege",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "pg_has_role",
        null_handling: NullHandling::Propagate,
        signature: sig_privilege_2_or_3,
        eval: eval_true,
    },
    ScalarFunction {
        name: "obj_description",
        null_handling: NullHandling::Nullable,
        signature: sig_obj_description,
        eval: eval_null,
    },
    ScalarFunction {
        name: "col_description",
        null_handling: NullHandling::Nullable,
        signature: sig_two_integers_to_text,
        eval: eval_null,
    },
    ScalarFunction {
        name: "pg_get_serial_sequence",
        null_handling: NullHandling::Nullable,
        signature: sig_two_text_to_text,
        eval: eval_pg_get_serial_sequence,
    },
    ScalarFunction {
        name: "pg_relation_size",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_integer,
        eval: eval_zero,
    },
    ScalarFunction {
        name: "pg_table_size",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_integer,
        eval: eval_zero,
    },
    ScalarFunction {
        name: "pg_indexes_size",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_integer,
        eval: eval_zero,
    },
    ScalarFunction {
        name: "pg_total_relation_size",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_integer,
        eval: eval_zero,
    },
    ScalarFunction {
        name: "pg_my_temp_schema",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_integer,
        eval: eval_zero,
    },
    ScalarFunction {
        name: "pg_is_other_temp_schema",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_boolean,
        eval: eval_false,
    },
    ScalarFunction {
        name: "to_regclass",
        null_handling: NullHandling::Nullable,
        signature: sig_text_to_integer,
        eval: eval_to_regclass,
    },
    ScalarFunction {
        name: "to_regtype",
        null_handling: NullHandling::Nullable,
        signature: sig_text_to_integer,
        eval: eval_to_regtype,
    },
    ScalarFunction {
        name: "pg_get_viewdef",
        null_handling: NullHandling::Nullable,
        signature: sig_oid_optional_bool_to_text,
        eval: eval_null,
    },
    ScalarFunction {
        name: "pg_get_functiondef",
        null_handling: NullHandling::Nullable,
        signature: sig_integer_to_text,
        eval: eval_pg_get_functiondef,
    },
    ScalarFunction {
        name: "pg_get_function_arguments",
        null_handling: NullHandling::Nullable,
        signature: sig_integer_to_text,
        eval: eval_pg_get_function_arguments,
    },
    ScalarFunction {
        name: "pg_get_function_result",
        null_handling: NullHandling::Nullable,
        signature: sig_integer_to_text,
        eval: eval_pg_get_function_result,
    },
    ScalarFunction {
        name: "pg_get_triggerdef",
        null_handling: NullHandling::Nullable,
        signature: sig_oid_optional_bool_to_text,
        eval: eval_null,
    },
    ScalarFunction {
        name: "pg_get_ruledef",
        null_handling: NullHandling::Nullable,
        signature: sig_oid_optional_bool_to_text,
        eval: eval_null,
    },
    ScalarFunction {
        name: "pg_get_partkeydef",
        null_handling: NullHandling::Nullable,
        signature: sig_integer_to_text,
        eval: eval_null,
    },
    ScalarFunction {
        name: "pg_function_is_visible",
        null_handling: NullHandling::Propagate,
        signature: sig_integer_to_boolean,
        eval: eval_pg_function_is_visible,
    },
    ScalarFunction {
        name: "oidvectortypes",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_oidvectortypes,
    },
];

// ---------------------------------------------------------------------------
// Signature checkers (bind time). Errors are `ErrorKind::Plan`.
// ---------------------------------------------------------------------------

fn sig_text_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Text)?;
    Ok(DataType::Text)
}

fn sig_length(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Text)?;
    Ok(DataType::Integer)
}

/// `ABS`/`FLOOR`/`CEIL`/`ROUND`: accept either numeric type and return that same
/// type.
fn sig_numeric_same(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    numeric_arg_type(name, &args[0])
}

/// `SQRT`: any numeric argument, widened to `DOUBLE`.
fn sig_numeric_to_double(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    numeric_arg_type(name, &args[0])?;
    Ok(DataType::Double)
}

/// `POWER`/`POW`: two numeric arguments, result `DOUBLE`.
fn sig_power(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    numeric_arg_type(name, &args[0])?;
    numeric_arg_type(name, &args[1])?;
    Ok(DataType::Double)
}

/// `MOD`: integer-only (matching the `%` operator, which rejects `DOUBLE`).
fn sig_mod(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Integer)?;
    require_arg_type(&args[1], DataType::Integer)?;
    Ok(DataType::Integer)
}

fn sig_replace(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 3)?;
    for arg in args {
        require_arg_type(arg, DataType::Text)?;
    }
    Ok(DataType::Text)
}

fn sig_position(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Text)?;
    Ok(DataType::Integer)
}

fn sig_text_integer_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Integer)?;
    Ok(DataType::Text)
}

/// `CONCAT`: variadic over one or more `TEXT` arguments.
fn sig_concat(_name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.is_empty() {
        return Err(plan_err(
            SqlState::SyntaxError,
            "concat requires at least one argument",
        ));
    }
    for arg in args {
        require_arg_type(arg, DataType::Text)?;
    }
    Ok(DataType::Text)
}

/// `SUBSTRING(text, start[, length])`.
fn sig_substring(_name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 2 && args.len() != 3 {
        return Err(plan_err(
            SqlState::SyntaxError,
            "substring expects 2 or 3 arguments",
        ));
    }
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Integer)?;
    if let Some(length) = args.get(2) {
        require_arg_type(length, DataType::Integer)?;
    }
    Ok(DataType::Text)
}

/// `EXTRACT(field FROM source)`, bound as `extract('field', source)`. The field
/// literal (when constant) must name a supported component; the source must be a
/// `DATE` or `TIMESTAMP`.
fn sig_extract(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    if let Some(Value::Text(field)) = args[0].literal
        && !is_supported_extract_field(field)
    {
        return Err(plan_err(
            SqlState::FeatureNotSupported,
            format!("EXTRACT field {field} is not supported"),
        ));
    }
    if !matches!(args[1].data_type, DataType::Date | DataType::Timestamp) {
        return Err(plan_err(
            SqlState::DatatypeMismatch,
            format!(
                "EXTRACT requires a date or timestamp argument, got {:?}",
                args[1].data_type
            ),
        ));
    }
    Ok(DataType::Double)
}

fn sig_no_args_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 0)?;
    Ok(DataType::Text)
}

fn sig_no_args_integer(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 0)?;
    Ok(DataType::Integer)
}

fn sig_no_args_timestamptz(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 0)?;
    Ok(DataType::TimestampTz)
}

fn sig_integer_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Integer)?;
    Ok(DataType::Text)
}

fn sig_integer_to_integer(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Integer)?;
    Ok(DataType::Integer)
}

fn sig_integer_to_boolean(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Integer)?;
    Ok(DataType::Boolean)
}

fn sig_text_to_integer(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Text)?;
    Ok(DataType::Integer)
}

fn sig_two_text_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Text)?;
    Ok(DataType::Text)
}

fn sig_two_integers_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Integer)?;
    require_arg_type(&args[1], DataType::Integer)?;
    Ok(DataType::Text)
}

fn sig_format_type(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Integer)?;
    require_arg_type(&args[1], DataType::Integer)?;
    Ok(DataType::Text)
}

fn sig_pg_get_indexdef(name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 1 && args.len() != 3 {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects 1 or 3 arguments"),
        ));
    }
    require_arg_type(&args[0], DataType::Integer)?;
    if args.len() == 3 {
        require_arg_type(&args[1], DataType::Integer)?;
        require_arg_type(&args[2], DataType::Boolean)?;
    }
    Ok(DataType::Text)
}

fn sig_pg_get_expr(name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 2 && args.len() != 3 {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects 2 or 3 arguments"),
        ));
    }
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Integer)?;
    if args.len() == 3 {
        require_arg_type(&args[2], DataType::Boolean)?;
    }
    Ok(DataType::Text)
}

fn sig_pg_get_constraintdef(name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 1 && args.len() != 2 {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects 1 or 2 arguments"),
        ));
    }
    require_arg_type(&args[0], DataType::Integer)?;
    if args.len() == 2 {
        require_arg_type(&args[1], DataType::Boolean)?;
    }
    Ok(DataType::Text)
}

fn sig_privilege_2_or_3(name: &str, args: &[ArgType]) -> Result<DataType> {
    sig_privilege_range(name, args, 2, 3)
}

fn sig_privilege_3_or_4(name: &str, args: &[ArgType]) -> Result<DataType> {
    sig_privilege_range(name, args, 3, 4)
}

fn sig_privilege_range(
    name: &str,
    args: &[ArgType],
    min_arity: usize,
    max_arity: usize,
) -> Result<DataType> {
    if !(min_arity..=max_arity).contains(&args.len()) {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects {min_arity} to {max_arity} arguments"),
        ));
    }
    for arg in args {
        require_text_or_integer_arg(name, arg)?;
    }
    Ok(DataType::Boolean)
}

fn sig_obj_description(name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 1 && args.len() != 2 {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects 1 or 2 arguments"),
        ));
    }
    require_arg_type(&args[0], DataType::Integer)?;
    if args.len() == 2 {
        require_arg_type(&args[1], DataType::Text)?;
    }
    Ok(DataType::Text)
}

fn sig_oid_optional_bool_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 1 && args.len() != 2 {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects 1 or 2 arguments"),
        ));
    }
    require_arg_type(&args[0], DataType::Integer)?;
    if args.len() == 2 {
        require_arg_type(&args[1], DataType::Boolean)?;
    }
    Ok(DataType::Text)
}

// ---------------------------------------------------------------------------
// Evaluators (run time). Errors are `ErrorKind::Execute`. Arity and argument
// types are already validated at bind time, so evaluators index arguments and
// read their expected types directly.
// ---------------------------------------------------------------------------

fn eval_upper(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Text(text_arg(&values[0])?.to_uppercase()))
}

fn eval_lower(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Text(text_arg(&values[0])?.to_lowercase()))
}

fn eval_trim(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Text(text_arg(&values[0])?.trim().to_string()))
}

fn eval_length(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let length = text_arg(&values[0])?.chars().count();
    i64::try_from(length)
        .map(Value::Integer)
        .map_err(|_| DbError::internal("string length exceeds i64 range"))
}

/// `ABS`: integer stays integer (with overflow checking); double uses `f64::abs`.
fn eval_abs(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    match &values[0] {
        Value::Integer(value) => value
            .checked_abs()
            .map(Value::Integer)
            .ok_or_else(integer_overflow),
        Value::Float(value) => Ok(Value::Float(value.0.abs().into())),
        _ => type_mismatch("abs requires a numeric argument"),
    }
}

fn eval_floor(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    numeric_round(&values[0], f64::floor)
}

fn eval_ceil(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    numeric_round(&values[0], f64::ceil)
}

fn eval_round(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    numeric_round(&values[0], f64::round_ties_even)
}

/// `SQRT(numeric)` → double. A negative argument is rejected (PostgreSQL raises
/// rather than returning NaN).
fn eval_sqrt(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let value = double_arg(&values[0])?;
    if value < 0.0 {
        return Err(exec_err(
            SqlState::NumericValueOutOfRange,
            "cannot take square root of a negative number",
        ));
    }
    Ok(Value::Float(value.sqrt().into()))
}

/// `POWER(base, exp)` → double. A non-finite result (overflow, or an undefined
/// case such as a negative base to a fractional power) is rejected.
fn eval_power(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let result = double_arg(&values[0])?.powf(double_arg(&values[1])?);
    if !result.is_finite() {
        return Err(exec_err(
            SqlState::NumericValueOutOfRange,
            "power result is out of range or undefined",
        ));
    }
    Ok(Value::Float(result.into()))
}

/// `MOD(a, b)` → integer remainder (`a % b`), with division-by-zero rejected.
fn eval_mod(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let left = integer_arg(&values[0])?;
    let right = integer_arg(&values[1])?;
    if right == 0 {
        return Err(exec_err(SqlState::DivisionByZero, "division by zero"));
    }
    // `i64::MIN % -1` overflows `checked_rem`, but the remainder is mathematically
    // 0; PostgreSQL returns 0 here rather than erroring.
    if right == -1 {
        return Ok(Value::Integer(0));
    }
    left.checked_rem(right)
        .map(Value::Integer)
        .ok_or_else(integer_overflow)
}

/// `REPLACE(string, from, to)`: replace every non-overlapping occurrence of
/// `from` with `to`. An empty `from` leaves the string unchanged (matching
/// PostgreSQL, unlike Rust's `str::replace`).
fn eval_replace(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let string = text_arg(&values[0])?;
    let from = text_arg(&values[1])?;
    let to = text_arg(&values[2])?;
    if from.is_empty() {
        Ok(Value::Text(string.to_string()))
    } else {
        Ok(Value::Text(string.replace(from, to)))
    }
}

/// `POSITION(substring, string)`: the 1-based character index of the first
/// occurrence of `substring` in `string`, or 0 if absent. An empty substring is
/// at position 1.
fn eval_position(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let needle: Vec<char> = text_arg(&values[0])?.chars().collect();
    let haystack: Vec<char> = text_arg(&values[1])?.chars().collect();
    let position = if needle.is_empty() {
        1
    } else if needle.len() > haystack.len() {
        0
    } else {
        (0..=haystack.len() - needle.len())
            .find(|&start| haystack[start..start + needle.len()] == needle[..])
            .map_or(0, |start| (start + 1) as i64)
    };
    Ok(Value::Integer(position))
}

fn eval_left(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    eval_left_right(values, true)
}

fn eval_right(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    eval_left_right(values, false)
}

/// `LEFT(string, n)` / `RIGHT(string, n)`, by character. A negative `n` removes
/// `|n|` characters from the far end (PostgreSQL semantics).
fn eval_left_right(values: &[Value], left: bool) -> Result<Value> {
    let chars: Vec<char> = text_arg(&values[0])?.chars().collect();
    let n = integer_arg(&values[1])?;
    let len = chars.len() as i64;
    let result: String = if left {
        // First `take` characters: `min(n, len)` for n >= 0, else all but the
        // last `|n|` (`len + n`), clamped to `[0, len]`.
        let take = if n >= 0 {
            n.min(len)
        } else {
            len.saturating_add(n).max(0)
        } as usize;
        chars[..take].iter().collect()
    } else {
        // Characters from `start` to the end: skip the first `len - n` for n >= 0
        // (keeping the last n), or skip the first `|n|` for n < 0.
        let start = if n >= 0 {
            len.saturating_sub(n).max(0)
        } else {
            n.saturating_neg().min(len)
        } as usize;
        chars[start..].iter().collect()
    };
    Ok(Value::Text(result))
}

/// `CONCAT(...)`: ignore NULL arguments and concatenate the rest; the result is
/// the empty string when every argument is NULL (never NULL).
fn eval_concat(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let mut out = String::new();
    for value in values {
        match value {
            Value::Null => {}
            Value::Text(text) => out.push_str(text),
            _ => return type_mismatch("concat requires text arguments"),
        }
    }
    Ok(Value::Text(out))
}

/// `SUBSTRING(text, start[, length])` with 1-based start positions, clamped to the
/// string bounds. A negative length is rejected.
fn eval_substring(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let chars: Vec<char> = text_arg(&values[0])?.chars().collect();
    let length = i64::try_from(chars.len())
        .map_err(|_| DbError::internal("string length exceeds i64 range"))?;
    let start = integer_arg(&values[1])?;

    // The result spans 1-based positions `lower..upper`, intersected with the
    // string's valid range `[1, length]`.
    let lower = start.max(1);
    let upper = match values.get(2) {
        Some(count) => {
            let count = integer_arg(count)?;
            if count < 0 {
                return type_mismatch("substring length must not be negative");
            }
            start.saturating_add(count).min(length + 1)
        }
        None => length + 1,
    };
    if upper <= lower {
        return Ok(Value::Text(String::new()));
    }

    // `lower >= 1` and `upper <= length + 1`, so both indices are in range.
    let begin = usize::try_from(lower - 1).map_err(|_| DbError::internal("substring index"))?;
    let end = usize::try_from(upper - 1).map_err(|_| DbError::internal("substring index"))?;
    Ok(Value::Text(chars[begin..end].iter().collect()))
}

/// `EXTRACT(field FROM source)`: the requested calendar/clock component of a DATE
/// or TIMESTAMP, returned as `DOUBLE PRECISION` (seconds include the fractional
/// part). DATE sources have zero-valued time components.
fn eval_extract(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    const MICROS_PER_SEC: i64 = 1_000_000;
    const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

    let field = text_arg(&values[0])?;
    let source = &values[1];
    let (year, month, day, hour, minute, second) = match source {
        Value::Date(days) => {
            let (year, month, day) = crate::datetime::civil_from_days(*days);
            (year as f64, month as f64, day as f64, 0.0, 0.0, 0.0)
        }
        Value::Timestamp(micros) => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let rest = micros.rem_euclid(MICROS_PER_DAY);
            let (year, month, day) = crate::datetime::civil_from_days(days);
            let total_secs = rest / MICROS_PER_SEC;
            let fraction = (rest % MICROS_PER_SEC) as f64 / MICROS_PER_SEC as f64;
            (
                year as f64,
                month as f64,
                day as f64,
                (total_secs / 3_600) as f64,
                ((total_secs % 3_600) / 60) as f64,
                (total_secs % 60) as f64 + fraction,
            )
        }
        _ => return type_mismatch("extract requires a date or timestamp argument"),
    };

    let Some(component) = ExtractField::parse(field) else {
        return Err(exec_err(
            SqlState::FeatureNotSupported,
            format!("EXTRACT field {field} is not supported"),
        ));
    };
    let value = match component {
        ExtractField::Year => year,
        ExtractField::Month => month,
        ExtractField::Day => day,
        ExtractField::Hour => hour,
        ExtractField::Minute => minute,
        ExtractField::Second => second,
    };
    Ok(Value::Float(value.into()))
}

fn eval_statement_timestamp(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::TimestampTz(ctx.statement_timestamp_micros))
}

fn eval_version(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text(format!(
        "PostgreSQL {} (SaguaroDB {})",
        POSTGRES_COMPAT_VERSION,
        env!("CARGO_PKG_VERSION")
    )))
}

fn eval_current_database(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text(ctx.session_info.database.clone()))
}

fn eval_current_schema(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text("public".to_string()))
}

fn eval_current_user(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text(ctx.session_info.user.clone()))
}

fn eval_pg_backend_pid(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Integer(i64::from(ctx.session_info.backend_pid)))
}

fn eval_current_setting(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let name = text_arg(&values[0])?;
    let Some(setting) = ctx.system_state.setting(name) else {
        return Err(exec_err(
            SqlState::UndefinedObject,
            format!("unrecognized configuration parameter \"{name}\""),
        ));
    };
    Ok(Value::Text(setting))
}

fn eval_format_type(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let oid = match &values[0] {
        Value::Null => return Ok(Value::Null),
        value => integer_arg(value)?,
    };
    let typmod = match &values[1] {
        Value::Null => -1,
        value => integer_arg(value)?,
    };
    Ok(Value::Text(
        PgType::from_oid_typmod(oid, typmod)
            .map(|pg_type| pg_type.format_type_name())
            .unwrap_or_else(|| "???".to_string()),
    ))
}

fn eval_pg_get_indexdef(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let index_oid = integer_arg(&values[0])?;
    let (column, pretty) = match values {
        [_] => (None, false),
        [_, column, pretty] => (Some(integer_arg(column)?), boolean_arg(pretty)?),
        _ => return type_mismatch("pg_get_indexdef expects 1 or 3 arguments"),
    };
    Ok(nullable_text(
        ctx.catalog_introspection
            .pg_get_indexdef(index_oid, column, pretty)?,
    ))
}

fn eval_pg_table_is_visible(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Boolean(
        ctx.catalog_introspection
            .pg_table_is_visible(integer_arg(&values[0])?)?,
    ))
}

fn eval_pg_get_expr(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let expr = text_arg(&values[0])?;
    let relation_oid = integer_arg(&values[1])?;
    let pretty = values.get(2).map(boolean_arg).transpose()?.unwrap_or(false);
    Ok(nullable_text(ctx.catalog_introspection.pg_get_expr(
        expr,
        relation_oid,
        pretty,
    )?))
}

fn eval_pg_get_constraintdef(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let constraint_oid = integer_arg(&values[0])?;
    let pretty = values.get(1).map(boolean_arg).transpose()?.unwrap_or(false);
    Ok(nullable_text(
        ctx.catalog_introspection
            .pg_get_constraintdef(constraint_oid, pretty)?,
    ))
}

fn eval_pg_get_userbyid(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(nullable_text(
        ctx.catalog_introspection
            .pg_get_userbyid(integer_arg(&values[0])?)?,
    ))
}

fn eval_pg_get_serial_sequence(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(nullable_text(
        ctx.catalog_introspection
            .pg_get_serial_sequence(text_arg(&values[0])?, text_arg(&values[1])?)?,
    ))
}

fn eval_to_regclass(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(nullable_int(
        ctx.catalog_introspection
            .to_regclass(text_arg(&values[0])?)?,
    ))
}

fn eval_to_regtype(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(nullable_int(PgType::oid_for_name(text_arg(&values[0])?)))
}

fn eval_pg_get_functiondef(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let Some(entry) = pg_proc_catalog_entry(integer_arg(&values[0])?) else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format!(
        "CREATE FUNCTION pg_catalog.{}({}) RETURNS {} LANGUAGE sql",
        entry.name,
        format_function_arguments(entry),
        format_type_oid(entry.ret_oid)
    )))
}

fn eval_pg_get_function_arguments(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let Some(entry) = pg_proc_catalog_entry(integer_arg(&values[0])?) else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format_function_arguments(entry)))
}

fn eval_pg_get_function_result(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let Some(entry) = pg_proc_catalog_entry(integer_arg(&values[0])?) else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format_type_oid(entry.ret_oid)))
}

fn eval_pg_function_is_visible(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Boolean(
        pg_proc_catalog_entry(integer_arg(&values[0])?).is_some(),
    ))
}

fn eval_oidvectortypes(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let rendered = text_arg(&values[0])?
        .split_whitespace()
        .map(|part| {
            part.parse::<i64>()
                .map(format_type_oid)
                .unwrap_or_else(|_| "???".to_string())
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Value::Text(rendered))
}

fn eval_true(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Boolean(true))
}

fn eval_false(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Boolean(false))
}

fn eval_zero(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Integer(0))
}

fn eval_null(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// The calendar/clock components `EXTRACT` supports — the single source of truth
/// for the accepted field names. `sig_extract` validates a literal field with
/// [`is_supported_extract_field`] (which is just `parse(..).is_some()`), and
/// `eval_extract` matches exhaustively on the parsed variant, so the set of
/// accepted names and the set of computable components cannot drift apart.
enum ExtractField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl ExtractField {
    fn parse(field: &str) -> Option<Self> {
        Some(match field {
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            _ => return None,
        })
    }
}

fn is_supported_extract_field(field: &str) -> bool {
    ExtractField::parse(field).is_some()
}

fn hint_candidate_types() -> &'static [DataType] {
    static CANDIDATES: OnceLock<Vec<DataType>> = OnceLock::new();
    CANDIDATES
        .get_or_init(|| {
            vec![
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
                    precision: None,
                    scale: 0,
                },
            ]
        })
        .as_slice()
}

fn signature_accepts_with_fixed_arg(
    func: &ScalarFunction,
    arity: usize,
    fixed_index: usize,
    fixed_type: &DataType,
) -> bool {
    fn search(
        func: &ScalarFunction,
        arity: usize,
        fixed_index: usize,
        fixed_type: &DataType,
        chosen: &mut Vec<DataType>,
    ) -> bool {
        if chosen.len() == arity {
            let args: Vec<ArgType> = chosen
                .iter()
                .cloned()
                .map(|data_type| ArgType {
                    data_type,
                    literal: None,
                })
                .collect();
            return (func.signature)(func.name, &args).is_ok();
        }
        if chosen.len() == fixed_index {
            chosen.push(fixed_type.clone());
            let accepted = search(func, arity, fixed_index, fixed_type, chosen);
            chosen.pop();
            return accepted;
        }
        for candidate in hint_candidate_types() {
            chosen.push(candidate.clone());
            if search(func, arity, fixed_index, fixed_type, chosen) {
                chosen.pop();
                return true;
            }
            chosen.pop();
        }
        false
    }

    search(func, arity, fixed_index, fixed_type, &mut Vec::new())
}

fn signature_accepts_uniform_args(
    func: &ScalarFunction,
    arity: usize,
    data_type: &DataType,
) -> bool {
    let args: Vec<_> = (0..arity)
        .map(|_| ArgType {
            data_type: data_type.clone(),
            literal: None,
        })
        .collect();
    (func.signature)(func.name, &args).is_ok()
}

fn plan_err(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}

fn exec_err(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::execute(code, message)
}

fn type_mismatch<T>(message: impl Into<String>) -> Result<T> {
    Err(exec_err(SqlState::DatatypeMismatch, message))
}

fn integer_overflow() -> DbError {
    exec_err(
        SqlState::NumericValueOutOfRange,
        "integer value out of range",
    )
}

fn expect_arity(name: &str, args: &[ArgType], arity: usize) -> Result<()> {
    if args.len() != arity {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects {arity} argument(s)"),
        ));
    }
    Ok(())
}

fn require_arg_type(arg: &ArgType, expected: DataType) -> Result<()> {
    if arg.data_type != expected {
        return Err(plan_err(
            SqlState::DatatypeMismatch,
            format!(
                "expected expression type {:?}, got {:?}",
                expected, arg.data_type
            ),
        ));
    }
    Ok(())
}

fn require_text_or_integer_arg(name: &str, arg: &ArgType) -> Result<()> {
    if matches!(arg.data_type, DataType::Text | DataType::Integer) {
        return Ok(());
    }
    Err(plan_err(
        SqlState::DatatypeMismatch,
        format!(
            "function {name} requires text or integer arguments, got {:?}",
            arg.data_type
        ),
    ))
}

/// The numeric type (`Integer` or `Double`) of an argument for functions that
/// accept either. Used both to validate (`SQRT`, `POWER`) and to carry the type
/// through (`ABS`, `FLOOR`, `CEIL`, `ROUND`).
fn numeric_arg_type(name: &str, arg: &ArgType) -> Result<DataType> {
    match arg.data_type {
        DataType::Integer => Ok(DataType::Integer),
        DataType::Double => Ok(DataType::Double),
        ref other => Err(plan_err(
            SqlState::DatatypeMismatch,
            format!("function {name} requires a numeric argument, got {other:?}"),
        )),
    }
}

fn text_arg(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => type_mismatch("function expected a text argument"),
    }
}

fn integer_arg(value: &Value) -> Result<i64> {
    match value {
        Value::Integer(value) => Ok(*value),
        _ => type_mismatch("function expected an integer argument"),
    }
}

fn boolean_arg(value: &Value) -> Result<bool> {
    match value {
        Value::Boolean(value) => Ok(*value),
        _ => type_mismatch("function expected a boolean argument"),
    }
}

fn nullable_text(value: Option<String>) -> Value {
    value.map(Value::Text).unwrap_or(Value::Null)
}

fn nullable_int(value: Option<i64>) -> Value {
    value.map(Value::Integer).unwrap_or(Value::Null)
}

fn format_function_arguments(entry: &PgProcCatalogEntry) -> String {
    let mut args = entry
        .arg_oids
        .iter()
        .map(|oid| format_type_oid(*oid))
        .collect::<Vec<_>>();
    if entry.variadic_oid() != 0
        && let Some(last) = args.last_mut()
    {
        *last = format!("VARIADIC {last}");
    }
    args.join(", ")
}

/// Read a numeric (`Integer` or `Double`) argument as `f64`.
fn double_arg(value: &Value) -> Result<f64> {
    match value {
        Value::Integer(value) => Ok(*value as f64),
        Value::Float(value) => Ok(value.0),
        _ => type_mismatch("function expected a numeric argument"),
    }
}

/// `FLOOR`/`CEIL`/`ROUND`: an integer is returned unchanged; a double is rounded
/// by `round` and stays double.
fn numeric_round(value: &Value, round: fn(f64) -> f64) -> Result<Value> {
    match value {
        Value::Integer(value) => Ok(Value::Integer(*value)),
        Value::Float(value) => Ok(Value::Float(round(value.0).into())),
        _ => type_mismatch("function requires a numeric argument"),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use crate::CatalogIntrospectionProvider;

    use super::*;

    fn arg(data_type: DataType) -> ArgType<'static> {
        ArgType {
            data_type,
            literal: None,
        }
    }

    fn result_type(name: &str, args: &[ArgType]) -> Result<DataType> {
        let func = lookup_scalar_function(name).expect("registered function");
        (func.signature)(func.name, args)
    }

    fn call(name: &str, values: &[Value]) -> Result<Value> {
        let ctx = StatementContext::new(0);
        let func = lookup_scalar_function(name).expect("registered function");
        (func.eval)(&ctx, values)
    }

    fn text_value(value: &str) -> Value {
        Value::Text(value.to_string())
    }

    #[test]
    fn lookup_misses_unregistered_names() {
        assert!(lookup_scalar_function("nextval").is_none());
        assert!(lookup_scalar_function("coalesce").is_none());
        assert!(lookup_scalar_function("count").is_none());
        assert!(lookup_scalar_function("bogus").is_none());
    }

    #[test]
    fn every_entry_has_a_lowercase_unique_name() {
        let mut seen = HashSet::new();
        for func in SCALAR_FUNCTIONS {
            assert_eq!(
                func.name,
                func.name.to_ascii_lowercase(),
                "name {} is not lowercase",
                func.name
            );
            assert!(seen.insert(func.name), "duplicate name {}", func.name);
        }
    }

    #[test]
    fn pg_proc_catalog_entries_stay_in_sync_with_scalar_registry() {
        let proc_names = PG_PROC_CATALOG_ENTRIES
            .iter()
            .map(|entry| entry.name)
            .collect::<HashSet<_>>();
        for func in SCALAR_FUNCTIONS {
            assert!(
                proc_names.contains(func.name),
                "registered scalar function {} is missing from pg_proc metadata",
                func.name
            );
        }

        let mut oids = HashSet::new();
        for entry in PG_PROC_CATALOG_ENTRIES {
            assert!(
                oids.insert(entry.oid),
                "duplicate pg_proc oid {}",
                entry.oid
            );
            assert!(
                lookup_scalar_function(entry.name).is_some(),
                "pg_proc metadata advertises unregistered function {}",
                entry.name
            );
            assert!(
                PgType::from_oid_typmod(entry.ret_oid, -1).is_some(),
                "pg_proc metadata for {} has unknown return type oid {}",
                entry.name,
                entry.ret_oid
            );
            for arg_oid in entry.arg_oids {
                assert!(
                    PgType::from_oid_typmod(*arg_oid, -1).is_some(),
                    "pg_proc metadata for {} has unknown arg type oid {}",
                    entry.name,
                    arg_oid
                );
            }
            if entry.variadic_oid() != 0 {
                assert!(
                    entry.arg_oids.contains(&entry.variadic_oid()),
                    "variadic pg_proc metadata for {} must include its variadic element type",
                    entry.name
                );
            }
        }

        let concat = PG_PROC_CATALOG_ENTRIES
            .iter()
            .find(|entry| entry.name == "concat")
            .expect("concat metadata");
        assert_eq!(concat.variadic_oid(), PG_PROC_TEXT);
        assert_eq!(format_function_arguments(concat), "VARIADIC text");
        let oidvectortypes = PG_PROC_CATALOG_ENTRIES
            .iter()
            .find(|entry| entry.name == "oidvectortypes")
            .expect("oidvectortypes metadata");
        assert_eq!(oidvectortypes.arg_oids, PG_PROC_OIDVECTOR_ARGS_1);
        assert_eq!(format_function_arguments(oidvectortypes), "oidvector");

        let has_signature = |name: &str, ret_oid: i64, arg_oids: &'static [i64]| {
            PG_PROC_CATALOG_ENTRIES.iter().any(|entry| {
                entry.name == name && entry.ret_oid == ret_oid && entry.arg_oids == arg_oids
            })
        };
        for (name, ret_oid, arg_oids) in [
            ("abs", PG_PROC_FLOAT8, PG_PROC_FLOAT8_ARGS_1),
            ("floor", PG_PROC_INT8, PG_PROC_INT8_ARGS_1),
            ("ceil", PG_PROC_INT8, PG_PROC_INT8_ARGS_1),
            ("ceiling", PG_PROC_INT8, PG_PROC_INT8_ARGS_1),
            ("round", PG_PROC_INT8, PG_PROC_INT8_ARGS_1),
            ("sqrt", PG_PROC_FLOAT8, PG_PROC_INT8_ARGS_1),
            ("power", PG_PROC_FLOAT8, PG_PROC_INT8_ARGS_2),
            ("power", PG_PROC_FLOAT8, PG_PROC_INT8_FLOAT8_ARGS),
            ("power", PG_PROC_FLOAT8, PG_PROC_FLOAT8_INT8_ARGS),
            ("pow", PG_PROC_FLOAT8, PG_PROC_INT8_ARGS_2),
            ("pow", PG_PROC_FLOAT8, PG_PROC_INT8_FLOAT8_ARGS),
            ("pow", PG_PROC_FLOAT8, PG_PROC_FLOAT8_INT8_ARGS),
        ] {
            assert!(
                has_signature(name, ret_oid, arg_oids),
                "{name} metadata should advertise {}({})",
                format_type_oid(ret_oid),
                arg_oids
                    .iter()
                    .map(|oid| format_type_oid(*oid))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        for name in [
            "has_table_privilege",
            "has_schema_privilege",
            "has_database_privilege",
            "has_sequence_privilege",
            "has_function_privilege",
            "has_any_column_privilege",
            "pg_has_role",
        ] {
            assert!(
                PG_PROC_CATALOG_ENTRIES.iter().any(|entry| {
                    entry.name == name && entry.arg_oids == PG_PROC_PRIV_OID_ARGS_2
                }),
                "{name} metadata should advertise the two-argument OID overload"
            );
            assert!(
                PG_PROC_CATALOG_ENTRIES.iter().any(|entry| {
                    entry.name == name && entry.arg_oids == PG_PROC_PRIV_OID_ARGS_3
                }),
                "{name} metadata should advertise the three-argument OID overload"
            );
        }
        assert!(PG_PROC_CATALOG_ENTRIES.iter().any(|entry| {
            entry.name == "has_column_privilege" && entry.arg_oids == PG_PROC_COLUMN_PRIV_OID_ARGS_3
        }));
        assert!(PG_PROC_CATALOG_ENTRIES.iter().any(|entry| {
            entry.name == "has_column_privilege" && entry.arg_oids == PG_PROC_COLUMN_PRIV_OID_ARGS
        }));
    }

    #[test]
    fn result_nullable_follows_null_handling() {
        let upper = lookup_scalar_function("upper").unwrap();
        assert!(upper.result_nullable([true]));
        assert!(upper.result_nullable([false, true]));
        assert!(!upper.result_nullable([false]));
        assert!(!upper.result_nullable([]));

        // NeverNull functions are non-nullable regardless of their arguments.
        let concat = lookup_scalar_function("concat").unwrap();
        assert!(!concat.result_nullable([true, true]));
        let version = lookup_scalar_function("version").unwrap();
        assert!(!version.result_nullable([]));

        let to_regclass = lookup_scalar_function("to_regclass").unwrap();
        assert!(to_regclass.result_nullable([false]));
        let format_type = lookup_scalar_function("format_type").unwrap();
        assert!(format_type.result_nullable([false, true]));
    }

    #[test]
    fn text_signatures_check_type_and_arity() {
        assert_eq!(
            result_type("upper", &[arg(DataType::Text)]).unwrap(),
            DataType::Text
        );
        let arity = result_type("upper", &[]).unwrap_err();
        assert_eq!(arity.code, SqlState::SyntaxError);
        let wrong_type = result_type("upper", &[arg(DataType::Integer)]).unwrap_err();
        assert_eq!(wrong_type.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn numeric_same_signature_carries_argument_type() {
        assert_eq!(
            result_type("abs", &[arg(DataType::Integer)]).unwrap(),
            DataType::Integer
        );
        assert_eq!(
            result_type("abs", &[arg(DataType::Double)]).unwrap(),
            DataType::Double
        );
        assert_eq!(
            result_type("abs", &[arg(DataType::Text)]).unwrap_err().code,
            SqlState::DatatypeMismatch
        );
    }

    #[test]
    fn extract_signature_validates_literal_field_and_source() {
        let field = Value::Text("year".to_string());
        let ok = result_type(
            "extract",
            &[
                ArgType {
                    data_type: DataType::Text,
                    literal: Some(&field),
                },
                arg(DataType::Timestamp),
            ],
        )
        .unwrap();
        assert_eq!(ok, DataType::Double);

        let bad_field = Value::Text("century".to_string());
        let err = result_type(
            "extract",
            &[
                ArgType {
                    data_type: DataType::Text,
                    literal: Some(&bad_field),
                },
                arg(DataType::Timestamp),
            ],
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        let bad_source =
            result_type("extract", &[arg(DataType::Text), arg(DataType::Integer)]).unwrap_err();
        assert_eq!(bad_source.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn concat_is_variadic_and_requires_an_argument() {
        assert_eq!(
            result_type("concat", &[arg(DataType::Text), arg(DataType::Text)]).unwrap(),
            DataType::Text
        );
        assert_eq!(
            result_type("concat", &[]).unwrap_err().code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn substring_accepts_two_or_three_arguments() {
        assert_eq!(
            result_type("substring", &[arg(DataType::Text), arg(DataType::Integer)]).unwrap(),
            DataType::Text
        );
        assert!(
            result_type(
                "substring",
                &[
                    arg(DataType::Text),
                    arg(DataType::Integer),
                    arg(DataType::Integer)
                ]
            )
            .is_ok()
        );
        assert_eq!(
            result_type("substring", &[arg(DataType::Text)])
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn system_functions_take_no_arguments() {
        assert_eq!(result_type("version", &[]).unwrap(), DataType::Text);
        assert_eq!(
            result_type("pg_backend_pid", &[]).unwrap(),
            DataType::Integer
        );
        assert_eq!(
            result_type("version", &[arg(DataType::Text)])
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn introspection_function_signatures_and_hints() {
        assert_eq!(
            result_type(
                "format_type",
                &[arg(DataType::Integer), arg(DataType::Integer)]
            )
            .unwrap(),
            DataType::Text
        );
        assert_eq!(
            result_type(
                "pg_get_indexdef",
                &[
                    arg(DataType::Integer),
                    arg(DataType::Integer),
                    arg(DataType::Boolean),
                ]
            )
            .unwrap(),
            DataType::Text
        );
        assert_eq!(
            result_type(
                "has_table_privilege",
                &[
                    arg(DataType::Text),
                    arg(DataType::Integer),
                    arg(DataType::Text),
                ]
            )
            .unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            result_type(
                "has_table_privilege",
                &[
                    arg(DataType::Text),
                    arg(DataType::Text),
                    arg(DataType::Text),
                    arg(DataType::Text),
                ]
            )
            .unwrap_err()
            .code,
            SqlState::SyntaxError
        );
        assert_eq!(
            result_type(
                "has_column_privilege",
                &[
                    arg(DataType::Text),
                    arg(DataType::Text),
                    arg(DataType::Text),
                    arg(DataType::Text),
                ]
            )
            .unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            result_type(
                "has_column_privilege",
                &[arg(DataType::Text), arg(DataType::Text)]
            )
            .unwrap_err()
            .code,
            SqlState::SyntaxError
        );
        assert_eq!(
            result_type("pg_function_is_visible", &[arg(DataType::Integer)]).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            result_type("oidvectortypes", &[arg(DataType::Text)]).unwrap(),
            DataType::Text
        );
        assert_eq!(
            result_type("format_type", &[arg(DataType::Integer)])
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
        assert_eq!(
            scalar_function_arg_hint("format_type", 2, 1),
            Some(DataType::Integer)
        );
        assert_eq!(
            scalar_function_arg_hint("pg_get_expr", 3, 2),
            Some(DataType::Boolean)
        );
        assert_eq!(
            scalar_function_arg_hint("current_setting", 1, 0),
            Some(DataType::Text)
        );
        assert_eq!(
            scalar_function_arg_hint("concat", 5, 4),
            Some(DataType::Text)
        );
        assert_eq!(scalar_function_arg_hint("pg_get_functiondef", 2, 1), None);
        assert_eq!(scalar_function_arg_hint("abs", 1, 0), None);
        assert_eq!(scalar_function_arg_hint("extract", 2, 1), None);
        assert_eq!(scalar_function_arg_hint("format_type", 1, 0), None);
    }

    #[derive(Debug)]
    struct TestIntrospection;

    impl CatalogIntrospectionProvider for TestIntrospection {
        fn pg_get_indexdef(
            &self,
            index_oid: i64,
            column: Option<i64>,
            pretty: bool,
        ) -> Result<Option<String>> {
            Ok(Some(format!(
                "index={index_oid} column={column:?} pretty={pretty}"
            )))
        }

        fn pg_get_constraintdef(
            &self,
            constraint_oid: i64,
            pretty: bool,
        ) -> Result<Option<String>> {
            Ok(Some(format!("constraint={constraint_oid} pretty={pretty}")))
        }

        fn pg_get_userbyid(&self, role_oid: i64) -> Result<Option<String>> {
            Ok((role_oid == 10).then(|| "alice".to_string()))
        }

        fn pg_table_is_visible(&self, relation_oid: i64) -> Result<bool> {
            Ok(relation_oid == 42)
        }

        fn to_regclass(&self, name: &str) -> Result<Option<i64>> {
            Ok((name == "users").then_some(42))
        }

        fn pg_get_serial_sequence(&self, table: &str, column: &str) -> Result<Option<String>> {
            Ok((table == "users" && column == "id").then(|| "users_id_seq".to_string()))
        }
    }

    #[test]
    fn introspection_evaluators_use_context_provider() {
        let ctx = StatementContext::new(0).with_catalog_introspection(Arc::new(TestIntrospection));
        let func = |name: &str, values: &[Value]| {
            let func = lookup_scalar_function(name).expect("registered function");
            (func.eval)(&ctx, values).unwrap()
        };

        assert_eq!(
            func("format_type", &[Value::Integer(23), Value::Integer(-1)]),
            Value::Text("integer".to_string())
        );
        assert_eq!(
            func("format_type", &[Value::Integer(23), Value::Null]),
            Value::Text("integer".to_string())
        );
        assert_eq!(
            func("format_type", &[Value::Null, Value::Integer(-1)]),
            Value::Null
        );
        assert_eq!(
            func(
                "pg_get_indexdef",
                &[Value::Integer(7), Value::Integer(1), Value::Boolean(true)]
            ),
            Value::Text("index=7 column=Some(1) pretty=true".to_string())
        );
        assert_eq!(
            func("pg_table_is_visible", &[Value::Integer(42)]),
            Value::Boolean(true)
        );
        assert_eq!(
            func("pg_get_constraintdef", &[Value::Integer(9)]),
            Value::Text("constraint=9 pretty=false".to_string())
        );
        assert_eq!(
            func("pg_get_userbyid", &[Value::Integer(10)]),
            Value::Text("alice".to_string())
        );
        assert_eq!(
            func(
                "pg_get_serial_sequence",
                &[text_value("users"), text_value("id")]
            ),
            Value::Text("users_id_seq".to_string())
        );
        assert_eq!(
            func("to_regclass", &[text_value("users")]),
            Value::Integer(42)
        );
        assert_eq!(
            func("to_regtype", &[text_value("integer")]),
            Value::Integer(23)
        );
        assert_eq!(
            func("to_regtype", &[text_value("_oid")]),
            Value::Integer(1028)
        );
        assert_eq!(
            func("pg_get_function_arguments", &[Value::Integer(14_022)]),
            Value::Text("oid, integer".to_string())
        );
        assert_eq!(
            func("pg_get_function_result", &[Value::Integer(14_022)]),
            Value::Text("text".to_string())
        );
        assert_eq!(
            func("pg_get_functiondef", &[Value::Integer(14_022)]),
            Value::Text(
                "CREATE FUNCTION pg_catalog.format_type(oid, integer) RETURNS text LANGUAGE sql"
                    .to_string()
            )
        );
        assert_eq!(
            func("pg_function_is_visible", &[Value::Integer(14_022)]),
            Value::Boolean(true)
        );
        assert_eq!(
            func("pg_function_is_visible", &[Value::Integer(999_999)]),
            Value::Boolean(false)
        );
        assert_eq!(
            func("oidvectortypes", &[text_value("20 25")]),
            Value::Text("bigint, text".to_string())
        );
        assert_eq!(func("obj_description", &[Value::Integer(1)]), Value::Null);
        assert_eq!(
            func(
                "has_table_privilege",
                &[Value::Integer(42), text_value("select")]
            ),
            Value::Boolean(true)
        );
    }

    #[test]
    fn statement_timestamp_functions_use_context_clock() {
        let ctx = StatementContext::new(0).with_statement_timestamp_micros(1_234_567);
        for name in ["current_timestamp", "now"] {
            assert_eq!(result_type(name, &[]).unwrap(), DataType::TimestampTz);
            assert_eq!(
                result_type(name, &[arg(DataType::Text)]).unwrap_err().code,
                SqlState::SyntaxError
            );

            let func = lookup_scalar_function(name).expect("registered function");
            assert!(!func.result_nullable([]));
            assert_eq!(
                (func.eval)(&ctx, &[]).unwrap(),
                Value::TimestampTz(1_234_567)
            );
        }
    }

    #[test]
    fn evaluators_compute_expected_values() {
        assert_eq!(
            call("upper", &[Value::Text("abc".to_string())]).unwrap(),
            Value::Text("ABC".to_string())
        );
        assert_eq!(
            call("length", &[Value::Text("héllo".to_string())]).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            call("abs", &[Value::Integer(-7)]).unwrap(),
            Value::Integer(7)
        );
        assert_eq!(
            call(
                "concat",
                &[
                    Value::Text("a".to_string()),
                    Value::Null,
                    Value::Text("b".to_string())
                ]
            )
            .unwrap(),
            Value::Text("ab".to_string())
        );
    }

    #[test]
    fn evaluators_surface_domain_errors() {
        assert_eq!(
            call("abs", &[Value::Integer(i64::MIN)]).unwrap_err().code,
            SqlState::NumericValueOutOfRange
        );
        assert_eq!(
            call("mod", &[Value::Integer(1), Value::Integer(0)])
                .unwrap_err()
                .code,
            SqlState::DivisionByZero
        );
        assert_eq!(
            call("sqrt", &[Value::Integer(-1)]).unwrap_err().code,
            SqlState::NumericValueOutOfRange
        );
    }

    #[test]
    fn durable_function_ids_validate_exact_and_variadic_signatures() {
        let concat_args = [DataType::Text, DataType::Text, DataType::Text];
        let concat = scalar_function_id("concat", &concat_args, &DataType::Text).unwrap();
        assert!(scalar_function_id_matches(
            concat,
            &concat_args,
            &DataType::Text,
            Some(&PgType::Text)
        ));

        let indexdef =
            scalar_function_id("pg_get_indexdef", &[DataType::Integer], &DataType::Text).unwrap();
        assert!(!scalar_function_id_matches(
            indexdef,
            &[DataType::Integer, DataType::Integer, DataType::Boolean],
            &DataType::Text,
            Some(&PgType::Text)
        ));
    }
}
