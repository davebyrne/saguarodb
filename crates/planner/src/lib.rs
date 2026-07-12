mod binder;
mod bound;
mod explain;
mod expr;
mod hoist;
mod logical;
mod params;
mod physical;
mod rewrite;
mod simplify;

pub use binder::{bind, bind_default_expr, bind_parameterized, bind_parameterized_with_pg_types};
pub use bound::{
    BoundDistinct, BoundFrom, BoundInsertSource, BoundOnConflict, BoundQuery, BoundQueryBody,
    BoundReturning, BoundSelect, BoundSelectItem, BoundSetOp, BoundStatement, BoundValues,
    CorrelatedColumn, DropTableTarget, OutputColumn,
};
pub use explain::format_explain;
pub use expr::{
    AggregateExpr, AggregateFunc, ApplyKind, BinOp, BoundExpr, BoundOrderByItem, JoinSide,
    JoinType, UnaryOp,
};
pub use logical::{LogicalPlan, logical_plan};
pub use params::{collect_param_pg_types, collect_param_types, substitute_params};
pub use parser::SetOp;
pub use physical::{PhysicalPlan, physical_plan};
pub use rewrite::{rewrite_expr, rewrite_plan_exprs};

pub fn mutates_sequences(statement: &BoundStatement) -> bool {
    match statement {
        BoundStatement::CreateTable { .. }
        | BoundStatement::DropTable { .. }
        | BoundStatement::AlterTableAddColumn { .. }
        | BoundStatement::AlterTableDropColumn { .. }
        | BoundStatement::AlterTableRenameColumn { .. }
        | BoundStatement::AlterTableRenameTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::DropIndex { .. }
        | BoundStatement::CreateSequence { .. }
        | BoundStatement::DropSequence { .. }
        | BoundStatement::CreateView { .. }
        | BoundStatement::DropView { .. }
        | BoundStatement::Copy { .. }
        | BoundStatement::Explain(_) => false,
        BoundStatement::Query(query) => query_mutates_sequences(query),
        BoundStatement::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            insert_source_mutates_sequences(source)
                || on_conflict
                    .as_ref()
                    .is_some_and(on_conflict_mutates_sequences)
                || returning.as_ref().is_some_and(returning_mutates_sequences)
        }
        BoundStatement::Update {
            assignments,
            source,
            returning,
            ..
        } => {
            assignments
                .iter()
                .any(|(_, expr)| expr_mutates_sequences(expr))
                || select_sequences(source, SequenceScan::Mutating)
                || returning.as_ref().is_some_and(returning_mutates_sequences)
        }
        BoundStatement::Delete {
            source, returning, ..
        } => {
            select_sequences(source, SequenceScan::Mutating)
                || returning.as_ref().is_some_and(returning_mutates_sequences)
        }
    }
}

fn returning_mutates_sequences(returning: &BoundReturning) -> bool {
    returning.exprs.iter().any(expr_mutates_sequences)
}

fn on_conflict_mutates_sequences(on_conflict: &BoundOnConflict) -> bool {
    match on_conflict {
        BoundOnConflict::DoNothing { .. } => false,
        BoundOnConflict::DoUpdate {
            assignments,
            filter,
            ..
        } => {
            assignments
                .iter()
                .any(|(_, expr)| expr_mutates_sequences(expr))
                || filter.as_ref().is_some_and(expr_mutates_sequences)
        }
    }
}

fn insert_source_mutates_sequences(source: &BoundInsertSource) -> bool {
    match source {
        BoundInsertSource::Values { rows, .. } => rows
            .iter()
            .flat_map(|row| row.iter())
            .any(expr_mutates_sequences),
        BoundInsertSource::Query(query) => query_mutates_sequences(query),
    }
}

/// Which sequence expressions a walk looks for: only the mutating ones
/// (`nextval`/`setval`), or any sequence function including `currval` —
/// the volatility set of `docs/specs/subqueries.md` §2.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SequenceScan {
    Mutating,
    Any,
}

/// Whether a bound query contains any sequence-function expression
/// (`nextval`, `setval`, `currval`) anywhere, including bodies of nested
/// subquery expressions. The Apply operator uses this to disable memoization.
pub fn query_contains_sequence_exprs(query: &BoundQuery) -> bool {
    query_sequences(query, SequenceScan::Any)
}

/// Whether evaluating a bound query advances or sets a sequence — its body plus
/// the query-level `ORDER BY` (which lives on the wrapper, not the `SELECT`).
fn query_mutates_sequences(query: &BoundQuery) -> bool {
    query_sequences(query, SequenceScan::Mutating)
}

fn query_sequences(query: &BoundQuery, scan: SequenceScan) -> bool {
    let body_matches = match &query.body {
        BoundQueryBody::Select(select) => select_sequences(select, scan),
        BoundQueryBody::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_sequences(expr, scan)),
        BoundQueryBody::SetOp(set_op) => {
            query_sequences(&set_op.left, scan) || query_sequences(&set_op.right, scan)
        }
    };
    body_matches
        || query
            .order_by
            .iter()
            .any(|item| expr_sequences(&item.expr, scan))
}

fn select_sequences(select: &BoundSelect, scan: SequenceScan) -> bool {
    select
        .columns
        .iter()
        .any(|item| expr_sequences(&item.expr, scan))
        || select
            .from
            .as_ref()
            .is_some_and(|from| from_sequences(from, scan))
        || select
            .filter
            .as_ref()
            .is_some_and(|expr| expr_sequences(expr, scan))
        || select
            .group_by
            .iter()
            .any(|expr| expr_sequences(expr, scan))
        || select
            .having
            .as_ref()
            .is_some_and(|expr| expr_sequences(expr, scan))
        || match &select.distinct {
            Some(BoundDistinct::On(exprs)) => exprs.iter().any(|expr| expr_sequences(expr, scan)),
            Some(BoundDistinct::All) | None => false,
        }
}

fn from_sequences(from: &BoundFrom, scan: SequenceScan) -> bool {
    match from {
        BoundFrom::Table { .. } | BoundFrom::System { .. } => false,
        BoundFrom::Derived { query, .. } | BoundFrom::View { query, .. } => {
            query_sequences(query, scan)
        }
        BoundFrom::Join {
            left,
            right,
            condition,
            ..
        } => {
            from_sequences(left, scan)
                || from_sequences(right, scan)
                || condition
                    .as_ref()
                    .is_some_and(|expr| expr_sequences(expr, scan))
        }
    }
}

fn expr_mutates_sequences(expr: &BoundExpr) -> bool {
    expr_sequences(expr, SequenceScan::Mutating)
}

fn expr_sequences(expr: &BoundExpr, scan: SequenceScan) -> bool {
    match expr {
        BoundExpr::Nextval { .. } | BoundExpr::Setval { .. } => true,
        BoundExpr::Currval { .. } => scan == SequenceScan::Any,
        BoundExpr::BinaryOp { left, right, .. } => {
            expr_sequences(left, scan) || expr_sequences(right, scan)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => expr_sequences(expr, scan),
        BoundExpr::Function { args, .. } => args.iter().any(|arg| expr_sequences(arg, scan)),
        BoundExpr::Array { elements, .. } => {
            elements.iter().any(|element| expr_sequences(element, scan))
        }
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            expr_sequences(array, scan) || subscripts.iter().any(|item| expr_sequences(item, scan))
        }
        BoundExpr::Any { left, array, .. } => {
            expr_sequences(left, scan) || expr_sequences(array, scan)
        }
        BoundExpr::AggregateCall { arg, .. } => {
            arg.as_deref().is_some_and(|arg| expr_sequences(arg, scan))
        }
        BoundExpr::InList { expr, list, .. } => {
            expr_sequences(expr, scan) || list.iter().any(|item| expr_sequences(item, scan))
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => expr_sequences(expr, scan) || expr_sequences(low, scan) || expr_sequences(high, scan),
        BoundExpr::Like { expr, pattern, .. } => {
            expr_sequences(expr, scan) || expr_sequences(pattern, scan)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(|op| expr_sequences(op, scan))
                || when_clauses
                    .iter()
                    .any(|(when, then)| expr_sequences(when, scan) || expr_sequences(then, scan))
                || else_clause
                    .as_deref()
                    .is_some_and(|expr| expr_sequences(expr, scan))
        }
        BoundExpr::InSubquery { expr, query, .. } => {
            expr_sequences(expr, scan) || query_sequences(query, scan)
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            query_sequences(query, scan)
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use catalog::{CatalogManager, MemoryCatalog, SystemView};
    use common::{
        CompressionSetting, CopyDirection, CopyFormat, CopyOptions, DataType, ErrorKind,
        IndexConstraintKind, ParsedColumnDef, PgType, SequenceOptions, SqlState, ToastCompression,
        ToastMode, ToastOptions, Value,
    };
    use parser::parse;

    use super::*;

    fn catalog_with_users() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        create_primary_key_index(&catalog, "users", &["id"]);
        catalog
    }

    fn create_primary_key_index(catalog: &MemoryCatalog, table: &str, columns: &[&str]) {
        catalog
            .create_index_with_constraint(
                format!("{table}_pkey"),
                table,
                &columns
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect::<Vec<_>>(),
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();
    }

    fn catalog_with_users_and_sequence() -> MemoryCatalog {
        let catalog = catalog_with_users();
        catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();
        catalog
    }

    /// `users` plus a non-unique secondary index `users_name` on `name`
    /// (index id 2, since the primary-key constraint index is catalog id 1).
    fn catalog_with_users_and_name_index() -> MemoryCatalog {
        let catalog = catalog_with_users();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        catalog
    }

    fn catalog_with_users_and_accounts() -> MemoryCatalog {
        let catalog = catalog_with_users();
        catalog
            .create_table(
                "accounts".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "owner".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        create_primary_key_index(&catalog, "accounts", &["id"]);
        catalog
    }

    fn catalog_with_text_key_table() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "codes".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "code".to_string(),
                        data_type: DataType::Text,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "label".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["code".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        create_primary_key_index(&catalog, "codes", &["code"]);
        catalog
    }

    #[test]
    fn binder_resolves_unqualified_column_to_input_ref_slot() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where name = 'Ada'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };

        assert_eq!(select.output_schema[0].name, "id");
        assert!(matches!(
            select.filter,
            Some(BoundExpr::BinaryOp { ref left, .. })
                if matches!(left.as_ref(), BoundExpr::InputRef { column: 1, slot: 1, .. })
        ));
    }

    fn catalog_with_typed_columns() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "t".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "small".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Int2),
                    },
                    ParsedColumnDef {
                        name: "txt".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: Some(10),
                        default: None,
                        pg_type: Some(PgType::Varchar(Some(10))),
                    },
                    ParsedColumnDef {
                        name: "big".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Int8),
                    },
                ],
                vec!["small".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        catalog
    }

    fn catalog_with_temporal_columns() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "t".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Int4),
                    },
                    ParsedColumnDef {
                        name: "ts".to_string(),
                        data_type: DataType::Timestamp,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Timestamp),
                    },
                    ParsedColumnDef {
                        name: "tstz".to_string(),
                        data_type: DataType::TimestampTz,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Timestamptz),
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        catalog
    }

    fn assert_timestamptz_to_timestamp_assignment_cast(expr: &BoundExpr) {
        let BoundExpr::Cast {
            expr: inner,
            data_type,
            pg_type,
            nullable,
        } = expr
        else {
            panic!("expected TIMESTAMPTZ -> TIMESTAMP assignment cast, got {expr:?}");
        };
        assert_eq!(data_type, &DataType::Timestamp);
        assert_eq!(pg_type, &PgType::Timestamp);
        assert_eq!(*nullable, inner.nullable());
        assert_eq!(inner.data_type(), DataType::TimestampTz);
    }

    #[test]
    fn output_schema_reports_column_reference_wire_types() {
        let catalog = catalog_with_typed_columns();
        let stmt = parse("select small, txt, big from t").unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected bound select");
        };

        // A bare column reference reports its source column's declared wire type.
        assert_eq!(select.output_schema[0].wire_type(), PgType::Int2);
        assert_eq!(
            select.output_schema[1].wire_type(),
            PgType::Varchar(Some(10))
        );
        assert_eq!(select.output_schema[2].wire_type(), PgType::Int8);
    }

    #[test]
    fn output_schema_reports_cast_and_expression_wire_types() {
        let catalog = catalog_with_typed_columns();
        let stmt =
            parse("select cast(small as bigint), cast(txt as varchar), big + 1 from t").unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected bound select");
        };

        // A CAST reports the target's wire type (character casts carry no length).
        assert_eq!(select.output_schema[0].wire_type(), PgType::Int8);
        assert_eq!(select.output_schema[1].wire_type(), PgType::Varchar(None));
        // A computed expression falls back to the natural type collapsed from its
        // result DataType (Integer => int8).
        assert_eq!(select.output_schema[2].wire_type(), PgType::Int8);
    }

    #[test]
    fn output_schema_preserves_oid_parameter_and_function_wire_types() {
        let catalog = catalog_with_users();
        let stmt =
            parse("select $1, to_regclass('users'), to_regtype('integer'), pg_my_temp_schema()")
                .unwrap();
        let (bound, params) =
            bind_parameterized_with_pg_types(&stmt, &catalog, &[Some(PgType::Oid)]).unwrap();
        assert_eq!(params, vec![DataType::Integer]);

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.output_schema[0].wire_type(), PgType::Oid);
        assert_eq!(select.output_schema[1].wire_type(), PgType::Oid);
        assert_eq!(select.output_schema[2].wire_type(), PgType::Oid);
        assert_eq!(select.output_schema[3].wire_type(), PgType::Oid);
    }

    #[test]
    fn catalog_function_placeholders_infer_oid_wire_types() {
        let catalog = catalog_with_users();
        let stmt =
            parse("select pg_get_indexdef($1), pg_table_is_visible($2), format_type($3, $4)")
                .unwrap();
        let (bound, params) =
            bind_parameterized_with_pg_types(&stmt, &catalog, &[None, None, None, None]).unwrap();
        assert_eq!(
            params,
            vec![
                DataType::Integer,
                DataType::Integer,
                DataType::Integer,
                DataType::Integer,
            ]
        );

        let param_pg_types =
            collect_param_pg_types(&bound, &params, &[None, None, None, None]).unwrap();
        assert_eq!(
            param_pg_types,
            vec![PgType::Oid, PgType::Oid, PgType::Oid, PgType::Int4]
        );
    }

    #[test]
    fn binder_binds_from_less_select() {
        let catalog = catalog_with_users();
        let stmt = parse("select 1 + 1 as n").unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected bound select");
        };

        // No FROM clause -> no source relation.
        assert!(select.from.is_none());
        assert_eq!(select.output_schema.len(), 1);
        assert_eq!(select.output_schema[0].name, "n");
        assert_eq!(select.output_schema[0].data_type, DataType::Integer);
    }

    #[test]
    fn binder_rejects_column_reference_without_from() {
        let catalog = catalog_with_users();
        // With no FROM there are no bindings, so a bare column cannot resolve.
        let err = bind(&parse("select id").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn binder_rejects_bare_wildcard_without_from() {
        let catalog = catalog_with_users();
        // `SELECT *` with no FROM has nothing to expand to (matches PostgreSQL);
        // it must not silently produce a zero-column result.
        let err = bind(&parse("select *").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_binds_values_body_with_unified_column_types() {
        let catalog = catalog_with_users();
        // Column types are inferred per column; a bare NULL adopts the column type
        // and makes the column nullable.
        let bound = bind(&parse("values (1, 'a'), (null, 'b')").unwrap(), &catalog).unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        let BoundQueryBody::Values(values) = &query.body else {
            panic!("expected a VALUES body");
        };
        assert_eq!(values.rows.len(), 2);
        let schema = query.output_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "column1");
        assert_eq!(schema[0].data_type, DataType::Integer);
        assert_eq!(schema[1].data_type, DataType::Text);
        // Nullability is exposed via output_columns: column1 has a NULL entry.
        let output = query.output_columns();
        assert!(output[0].nullable);
        assert!(!output[1].nullable);
    }

    #[test]
    fn binder_rejects_values_type_mismatch_across_rows() {
        let catalog = catalog_with_users();
        let err = bind(&parse("values (1), ('a')").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_rejects_values_rows_of_differing_width() {
        let catalog = catalog_with_users();
        let err = bind(&parse("values (1, 2), (3)").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_values_all_null_column() {
        let catalog = catalog_with_users();
        // An all-NULL column has no inferable type under the strict no-cast rule.
        let err = bind(&parse("values (null), (null)").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_binds_values_order_by_to_output_position() {
        let catalog = catalog_with_users();
        // `ORDER BY 1` over a VALUES binds to a LocalRef at that output slot,
        // evaluated by a Sort above the Values node.
        let BoundStatement::Query(query) =
            bind(&parse("values (3), (1), (2) order by 1").unwrap(), &catalog).unwrap()
        else {
            panic!("expected a query");
        };
        assert_eq!(query.order_by.len(), 1);
        assert!(matches!(
            query.order_by[0].expr,
            BoundExpr::LocalRef { slot: 0, .. }
        ));
    }

    #[test]
    fn binder_binds_values_derived_table_columns() {
        let catalog = catalog_with_users();
        let bound = bind(
            &parse("select t.x from (values (10), (20)) as t(x)").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        // The derived table exposes its VALUES columns (renamed to `x`) to the
        // outer scope, so the projection resolves and is typed Integer.
        assert_eq!(query.output_schema()[0].name, "x");
        assert_eq!(query.output_schema()[0].data_type, DataType::Integer);
    }

    #[test]
    fn binder_reconciles_set_operation_output_columns() {
        let catalog = catalog_with_users();
        let BoundStatement::Query(query) =
            bind(&parse("select 1 as x union select 2").unwrap(), &catalog).unwrap()
        else {
            panic!("expected a query");
        };
        let BoundQueryBody::SetOp(set_op) = &query.body else {
            panic!("expected a set operation");
        };
        assert!(matches!(set_op.op, SetOp::Union));
        assert!(!set_op.all);
        // The result column name comes from the left arm; the type is the shared
        // Integer of both arms.
        assert_eq!(query.output_schema()[0].name, "x");
        assert_eq!(query.output_schema()[0].data_type, DataType::Integer);
    }

    #[test]
    fn binder_rejects_set_operation_column_count_mismatch() {
        let catalog = catalog_with_users();
        let err = bind(&parse("select 1 union select 1, 2").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_set_operation_type_mismatch() {
        let catalog = catalog_with_users();
        let err = bind(&parse("select 1 union select 'x'").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_binds_intersect_all() {
        let catalog = catalog_with_users();
        // INTERSECT ALL / EXCEPT ALL now bind (multiset semantics run in the
        // executor); `all` is carried on the bound set operation.
        let BoundStatement::Query(query) =
            bind(&parse("select 1 intersect all select 2").unwrap(), &catalog).unwrap()
        else {
            panic!("expected a query");
        };
        let BoundQueryBody::SetOp(set_op) = &query.body else {
            panic!("expected a set operation");
        };
        assert!(matches!(set_op.op, SetOp::Intersect));
        assert!(set_op.all);
    }

    #[test]
    fn binder_types_null_set_operation_column_from_sibling_arm() {
        let catalog = catalog_with_users();
        // The right arm's bare NULL adopts the left arm's Integer, and the result
        // column is nullable because that arm contributes a NULL.
        let BoundStatement::Query(query) =
            bind(&parse("select 1 union select null").unwrap(), &catalog).unwrap()
        else {
            panic!("expected a query");
        };
        assert_eq!(query.output_schema()[0].data_type, DataType::Integer);
        assert!(query.output_columns()[0].nullable);
    }

    #[test]
    fn binder_rejects_set_operation_column_null_in_both_arms() {
        let catalog = catalog_with_users();
        // Neither arm can supply a type, so the NULL column stays untyped.
        let err = bind(&parse("select null union select null").unwrap(), &catalog).unwrap_err();
        assert!(matches!(
            err.code,
            SqlState::DatatypeMismatch | SqlState::SyntaxError
        ));
    }

    #[test]
    fn binder_set_operation_surfaces_a_real_arm_error_not_masked_by_null_typing() {
        let catalog = catalog_with_users();
        // The NULL-typing fallback re-binds an arm; a genuine error in that arm (an
        // unknown column) must surface as itself, not be masked, since the expected
        // types only ever type a bare NULL — this guards the fallback's safety
        // invariant against a future change that widens what `expected` influences.
        let err = bind(
            &parse("select nonexistent_col union select 1").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn binder_rejects_deeply_nested_untypeable_set_operation_in_polynomial_time() {
        let catalog = catalog_with_users();
        // A never-typeable left-associative chain: the leftmost arm's second column
        // is a bare NULL that no single-column sibling can type. The NULL-typing
        // fallback re-binds an arm on failure; that re-bind must stay single-pass so
        // total work is polynomial in the nesting depth. A prior version re-bound
        // each nested arm on every level, doubling work per level (exponential); at
        // this depth it rejected in ~4.5s (and minutes a little deeper) instead of
        // microseconds. The wall-clock bound is what actually guards the fix: the
        // correct code takes tens of microseconds, so a 1s bound leaves a ~10,000x
        // margin (robust to CI noise) while the exponential regression blows past it.
        let mut sql = String::from("select null, null");
        for _ in 0..24 {
            sql.push_str(" union select 1");
        }
        let statement = parse(&sql).unwrap();
        let start = std::time::Instant::now();
        let err = bind(&statement, &catalog).unwrap_err();
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "binding took {:?} — the exponential re-bind blowup may have returned",
            start.elapsed()
        );
        assert!(matches!(
            err.code,
            SqlState::DatatypeMismatch | SqlState::SyntaxError
        ));
    }

    #[test]
    fn binder_binds_set_operation_order_by_to_output_position() {
        let catalog = catalog_with_users();
        // `ORDER BY x` (an output-column name) binds to a LocalRef at that output
        // slot, evaluated by the Sort above the set operation.
        let BoundStatement::Query(query) = bind(
            &parse("select 1 as x union select 2 order by x").unwrap(),
            &catalog,
        )
        .unwrap() else {
            panic!("expected a query");
        };
        assert_eq!(query.order_by.len(), 1);
        assert!(matches!(
            query.order_by[0].expr,
            BoundExpr::LocalRef { slot: 0, .. }
        ));
    }

    #[test]
    fn binder_binds_cte_reference_as_inlined_derived_table() {
        let catalog = catalog_with_users();
        let bound = bind(
            &parse("with t as (select id from users) select id from t").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = &bound
        else {
            panic!("expected a select query");
        };
        // The CTE reference resolves to an inlined derived table (no dedicated node).
        assert!(matches!(select.from, Some(BoundFrom::Derived { .. })));
        assert_eq!(select.output_schema[0].name, "id");
    }

    #[test]
    fn binder_rejects_duplicate_cte_name() {
        let catalog = catalog_with_users();
        let err = bind(
            &parse("with t as (select 1), t as (select 2) select 1").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_cte_shadows_catalog_table() {
        let catalog = catalog_with_users();
        // `users` here refers to the CTE, not the catalog table, so the single
        // output column is the CTE's `x`, not the table's columns.
        let bound = bind(
            &parse("with users as (select 1 as x) select x from users").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        assert_eq!(query.output_schema().len(), 1);
        assert_eq!(query.output_schema()[0].name, "x");
    }

    #[test]
    fn binder_schema_qualified_public_table_bypasses_cte_shadow() {
        let catalog = catalog_with_users();
        let bound = bind(
            &parse("with users as (select 1 as x) select id from public.users").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        assert_eq!(query.output_schema().len(), 1);
        assert_eq!(query.output_schema()[0].name, "id");
    }

    #[test]
    fn binder_schema_qualified_public_system_named_table_is_not_system_fallback() {
        let catalog = catalog_with_users();
        let err = bind(&parse("select * from public.pg_class").unwrap(), &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_binds_system_views() {
        let catalog = catalog_with_users();
        let bound = bind(
            &parse("select oid, relname from pg_catalog.pg_class").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        let BoundQueryBody::Select(select) = &query.body else {
            panic!("expected select");
        };
        let Some(BoundFrom::System {
            view,
            alias,
            schema,
            ..
        }) = &select.from
        else {
            panic!("expected system view from item");
        };
        assert_eq!(*view, SystemView::PgClass);
        assert_eq!(alias, &None);
        assert_eq!(schema[0].name, "oid");
        assert_eq!(query.output_schema()[0].name, "oid");
        assert!(
            query
                .output_schema()
                .iter()
                .all(|column| column.table_id.is_none())
        );
    }

    #[test]
    fn binder_binds_information_schema_only_when_schema_qualified() {
        let catalog = catalog_with_users();
        let bound = bind(
            &parse("select table_name from information_schema.tables").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        let BoundQueryBody::Select(select) = &query.body else {
            panic!("expected select");
        };
        let Some(BoundFrom::System { view, .. }) = &select.from else {
            panic!("expected system view from item");
        };
        assert_eq!(*view, SystemView::InformationSchemaTables);

        let err = bind(&parse("select * from columns").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_user_table_shadows_bare_system_catalog_name() {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "pg_class".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                }],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        let bound = bind(&parse("select id from pg_class").unwrap(), &catalog).unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        let BoundQueryBody::Select(select) = &query.body else {
            panic!("expected select");
        };
        assert!(matches!(select.from, Some(BoundFrom::Table { .. })));

        let bound = bind(
            &parse("select oid from pg_catalog.pg_class").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(query) = &bound else {
            panic!("expected a query");
        };
        let BoundQueryBody::Select(select) = &query.body else {
            panic!("expected select");
        };
        assert!(matches!(
            select.from,
            Some(BoundFrom::System {
                view: SystemView::PgClass,
                ..
            })
        ));
    }

    #[test]
    fn binder_rejects_unknown_schema() {
        let catalog = catalog_with_users();

        let err = bind(&parse("select * from nosuch.users").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::InvalidSchemaName);
    }

    #[test]
    fn binder_rejects_modifying_bare_system_catalog() {
        let catalog = catalog_with_users();

        let err = bind(&parse("insert into pg_class values (1)").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn from_less_select_lowers_to_projection_over_unit_row() {
        let catalog = catalog_with_users();
        let bound = bind(&parse("select 1").unwrap(), &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();

        // A FROM-less projection lowers to a Projection over a single-row,
        // zero-column Values node (the unit relation the executor already knows).
        let LogicalPlan::Projection { source, .. } = &logical else {
            panic!("expected projection, got {logical:?}");
        };
        let LogicalPlan::Values {
            rows,
            output_schema,
        } = source.as_ref()
        else {
            panic!("expected a unit Values source, got {source:?}");
        };
        assert_eq!(rows.len(), 1, "exactly one unit row");
        assert!(rows[0].is_empty(), "the unit row has zero columns");
        assert!(output_schema.is_empty());
    }

    #[test]
    fn binder_resolves_order_by_ordinal_to_output_column() {
        let catalog = catalog_with_users();
        let stmt = parse("select name, id from users order by 2").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery { order_by, .. }) = bound else {
            panic!("expected bound select");
        };

        assert_eq!(order_by.len(), 1);
        // Output column 2 is `id`, which resolves to InputRef column 0, slot 0 —
        // not the constant literal 2.
        assert!(matches!(
            order_by[0].expr,
            BoundExpr::InputRef {
                column: 0,
                slot: 0,
                ..
            }
        ));
        assert!(order_by[0].ascending);
    }

    #[test]
    fn binder_rejects_out_of_range_order_by_ordinal() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users order by 2").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_ambiguous_unqualified_column() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select id from users join accounts on users.id = accounts.id").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn binder_marks_outer_join_null_supplying_side_nullable() {
        let catalog = catalog_with_users_and_accounts();
        for (sql, expected) in [
            (
                "select users.id, accounts.id from users left join accounts on users.id = accounts.id",
                [false, true],
            ),
            (
                "select users.id, accounts.id from users right join accounts on users.id = accounts.id",
                [true, false],
            ),
            (
                "select users.id, accounts.id from users full join accounts on users.id = accounts.id",
                [true, true],
            ),
        ] {
            let bound = bind(&parse(sql).unwrap(), &catalog).unwrap();
            let BoundStatement::Query(query) = bound else {
                panic!("expected query");
            };
            let nullability = query
                .output_columns()
                .into_iter()
                .map(|column| column.nullable)
                .collect::<Vec<_>>();
            assert_eq!(nullability, expected, "{sql}");
        }
    }

    #[test]
    fn binder_binds_insert_select_to_query_source() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users select id, name from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        assert!(matches!(
            bound,
            BoundStatement::Insert {
                source: BoundInsertSource::Query(_),
                ..
            }
        ));
    }

    #[test]
    fn binder_preserves_serial_marker_on_columns() {
        let catalog = MemoryCatalog::empty();
        let stmt = parse("create table users (id serial primary key, name text)").unwrap();
        let BoundStatement::CreateTable { columns, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected create table");
        };

        // SERIAL-ness lives solely in the column's parsed default; CREATE TABLE
        // execution derives the owned sequence from this marker.
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].default, Some(common::ParsedDefault::Serial));
        assert_eq!(columns[1].default, None);
    }

    #[test]
    fn bind_create_table_resolves_compression_default() {
        let catalog = MemoryCatalog::empty();
        let stmt =
            parse("create table t (id integer primary key) with (compression = 'zstd')").unwrap();
        let BoundStatement::CreateTable { compression, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected CreateTable");
        };
        assert_eq!(compression, CompressionSetting::Zstd);

        let stmt = parse("create table t (id integer primary key)").unwrap();
        let BoundStatement::CreateTable { compression, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected CreateTable");
        };
        assert_eq!(compression, CompressionSetting::None);
    }

    #[test]
    fn bind_create_table_resolves_toast_options() {
        let catalog = MemoryCatalog::empty();

        let stmt = parse("create table t (id integer primary key)").unwrap();
        let BoundStatement::CreateTable { toast, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected CreateTable");
        };
        assert_eq!(toast, ToastOptions::default_new_table());

        let stmt =
            parse("create table t (id integer primary key) with (toast = aggressive)").unwrap();
        let BoundStatement::CreateTable { toast, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected CreateTable");
        };
        assert_eq!(toast.mode, ToastMode::Aggressive);
        assert_eq!(
            toast.min_value_size,
            ToastOptions::AGGRESSIVE_TOAST_MIN_VALUE_SIZE
        );

        let stmt = parse(
            "create table t (id integer primary key) with \
             (toast = aggressive, toast_min_value_size = 777, toast_compression = none)",
        )
        .unwrap();
        let BoundStatement::CreateTable { toast, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected CreateTable");
        };
        assert_eq!(toast.mode, ToastMode::Aggressive);
        assert_eq!(toast.min_value_size, 777);
        assert_eq!(toast.compression, ToastCompression::None);
        assert_eq!(toast.active_dict_id, None);
    }

    #[test]
    fn bind_create_table_rejects_qualified_check_column_refs() {
        let catalog = MemoryCatalog::empty();
        let stmt = parse("create table t (id integer, check (t.id > 0))").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn alter_table_does_not_bind() {
        let catalog = MemoryCatalog::empty();
        let stmt = parse("alter table t set (compression = 'zstd')").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn binder_binds_returning_for_dml() {
        let catalog = catalog_with_users();

        // INSERT ... RETURNING id, name binds two output columns over the table.
        let stmt =
            parse("insert into users (id, name) values (1, 'Ada') returning id, name").unwrap();
        let BoundStatement::Insert {
            returning: Some(returning),
            ..
        } = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected insert with returning");
        };
        assert_eq!(returning.exprs.len(), 2);
        assert_eq!(returning.output_schema[0].name, "id");
        assert_eq!(returning.output_schema[1].name, "name");

        // UPDATE ... RETURNING * expands to every table column.
        let stmt = parse("update users set name = 'x' returning *").unwrap();
        let BoundStatement::Update {
            returning: Some(returning),
            ..
        } = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected update with returning");
        };
        assert_eq!(returning.output_schema.len(), 2);

        // RETURNING cannot contain aggregate calls.
        let stmt = parse("delete from users returning count(*)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);

        // A RETURNING expression referencing an unknown column is rejected.
        let stmt = parse("insert into users (id) values (1) returning missing").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn binder_binds_on_conflict() {
        let catalog = catalog_with_users();

        // ON CONFLICT DO NOTHING binds the current primary-key arbiter when present.
        let stmt =
            parse("insert into users (id, name) values (1, 'Ada') on conflict do nothing").unwrap();
        let BoundStatement::Insert {
            on_conflict:
                Some(BoundOnConflict::DoNothing {
                    target: Some(target),
                }),
            ..
        } = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected insert with DO NOTHING");
        };
        assert_eq!(target, vec![0]);

        // ON CONFLICT (id) DO UPDATE SET name = excluded.name binds an assignment
        // whose value references the excluded (proposed) row at slot n+1.
        let stmt = parse(
            "insert into users (id, name) values (1, 'Ada') \
             on conflict (id) do update set name = excluded.name",
        )
        .unwrap();
        let BoundStatement::Insert {
            on_conflict: Some(BoundOnConflict::DoUpdate { assignments, .. }),
            ..
        } = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected insert with DO UPDATE");
        };
        assert_eq!(assignments.len(), 1);
        // users = (id slot 0, name slot 1); excluded.name is slot 2+1 = 3.
        assert!(matches!(
            assignments[0],
            (1, BoundExpr::InputRef { slot: 3, .. })
        ));

        // A bare column in DO UPDATE resolves to the target row (slot 1), not
        // ambiguously to excluded.
        let stmt = parse(
            "insert into users (id, name) values (1, 'Ada') \
             on conflict (id) do update set name = name",
        )
        .unwrap();
        let BoundStatement::Insert {
            on_conflict: Some(BoundOnConflict::DoUpdate { assignments, .. }),
            ..
        } = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected DO UPDATE");
        };
        assert!(matches!(
            assignments[0],
            (1, BoundExpr::InputRef { slot: 1, .. })
        ));

        // A non-primary-key arbiter is rejected (only the PK is supported).
        let stmt = parse(
            "insert into users (id, name) values (1, 'Ada') \
             on conflict (name) do update set name = excluded.name",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        // DO UPDATE requires a conflict target.
        let stmt = parse(
            "insert into users (id, name) values (1, 'Ada') \
             on conflict do update set name = excluded.name",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        // Primary-key columns can be assigned because storage identity is hidden;
        // the primary-key constraint index enforces uniqueness.
        let stmt = parse(
            "insert into users (id, name) values (1, 'Ada') \
             on conflict (id) do update set id = excluded.id",
        )
        .unwrap();
        let BoundStatement::Insert {
            on_conflict: Some(BoundOnConflict::DoUpdate { assignments, .. }),
            ..
        } = bind(&stmt, &catalog).unwrap()
        else {
            panic!("expected DO UPDATE");
        };
        assert!(matches!(
            assignments[0],
            (0, BoundExpr::InputRef { slot: 2, .. })
        ));
    }

    #[test]
    fn binder_rejects_insert_select_that_omits_non_null_column() {
        let catalog = catalog_with_users();
        // `id` is NOT NULL and absent from the column list, so the insert is
        // rejected before the query source is considered.
        let stmt = parse("insert into users (name) select name from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::NotNullViolation);
    }

    #[test]
    fn binder_rejects_insert_select_with_mismatched_column_count() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users select id from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_rejects_insert_select_with_mismatched_types() {
        let catalog = catalog_with_users();
        // The query yields (name: text, id: integer) but the table expects
        // (id: integer, name: text), so the first column type mismatches.
        let stmt = parse("insert into users select name, id from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_rejects_unknown_functions_for_v1() {
        let catalog = catalog_with_users();
        let stmt = parse("select frobnicate(name) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_types_scalar_functions() {
        let catalog = catalog_with_users();
        let stmt =
            parse("select upper(name), length(name), abs(id), substring(name, 1, 2) from users")
                .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        // upper(text)->text nullable (name is nullable); length(text)->int;
        // abs(non-null int)->non-null int; substring(text,..)->text.
        assert_eq!(select.output_schema[0].data_type, DataType::Text);
        assert_eq!(select.output_schema[1].data_type, DataType::Integer);
        assert_eq!(select.output_schema[2].data_type, DataType::Integer);
        assert_eq!(select.output_schema[3].data_type, DataType::Text);
        assert!(matches!(
            select.columns[2].expr,
            BoundExpr::Function {
                nullable: false,
                ..
            }
        ));
    }

    #[test]
    fn binder_types_current_setting_and_infers_parameter() {
        let catalog = catalog_with_users();
        let stmt = parse("select current_setting($1), current_setting(null)").unwrap();
        let (bound, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();

        assert_eq!(params, vec![DataType::Text]);
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.output_schema[0].data_type, DataType::Text);
        assert_eq!(select.output_schema[1].data_type, DataType::Text);
        assert!(matches!(
            select.columns[0].expr,
            BoundExpr::Function {
                nullable: false,
                ..
            }
        ));
        assert!(matches!(
            select.columns[1].expr,
            BoundExpr::Function { nullable: true, .. }
        ));
    }

    #[test]
    fn binder_uses_registry_type_hints_for_introspection_functions() {
        let catalog = catalog_with_users();
        let stmt = parse("select pg_catalog.format_type(null, $1)").unwrap();
        let (bound, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();

        assert_eq!(params, vec![DataType::Integer]);
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.output_schema[0].data_type, DataType::Text);
        assert!(matches!(
            &select.columns[0].expr,
            BoundExpr::Function {
                name,
                nullable: true,
                ..
            } if name == "format_type"
        ));
    }

    #[test]
    fn binder_types_system_information_functions() {
        let catalog = catalog_with_users();
        let stmt = parse(
            "select version(), current_database(), current_catalog, current_user, \
             session_user, user, pg_backend_pid()",
        )
        .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        let names = select
            .columns
            .iter()
            .map(|item| match &item.expr {
                BoundExpr::Function { name, nullable, .. } => {
                    assert!(!nullable);
                    name.as_str()
                }
                expr => panic!("expected function expression, got {expr:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "version",
                "current_database",
                "current_catalog",
                "current_user",
                "session_user",
                "user",
                "pg_backend_pid",
            ]
        );
        for column in &select.output_schema[..6] {
            assert_eq!(column.data_type, DataType::Text);
        }
        assert_eq!(select.output_schema[6].data_type, DataType::Integer);
    }

    #[test]
    fn binder_binds_current_schema_fallback_but_prefers_real_column() {
        let empty = MemoryCatalog::empty();
        let bound = bind(&parse("select current_schema").unwrap(), &empty).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            &select.columns[0].expr,
            BoundExpr::Function {
                name,
                data_type,
                nullable,
                ..
            } if name == "current_schema" && *data_type == DataType::Text && !*nullable
        ));

        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "schemas".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "current_schema".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let bound = bind(
            &parse("select current_schema from schemas").unwrap(),
            &catalog,
        )
        .unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            &select.columns[0].expr,
            BoundExpr::InputRef {
                data_type,
                nullable,
                ..
            } if *data_type == DataType::Text && *nullable
        ));

        catalog
            .create_table(
                "other_schemas".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "current_schema".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let err = bind(
            &parse("select current_schema from schemas, other_schemas").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
        assert!(err.message.contains("ambiguous"));
    }

    #[test]
    fn binder_rejects_system_information_function_arguments() {
        let catalog = catalog_with_users();
        let err = bind(&parse("select version(1)").unwrap(), &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_binds_sequence_functions_and_mutation_classifier() {
        let catalog = catalog_with_users_and_sequence();
        let sequence = catalog
            .get_sequence_by_name("users_id_seq")
            .unwrap()
            .unwrap();
        let stmt = parse(
            "select nextval('users_id_seq'), currval('users_id_seq'), \
             setval('users_id_seq', 9, false) from users",
        )
        .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = &bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            &select.columns[0].expr,
            BoundExpr::Nextval {
                sequence: id,
                data_type,
                nullable
            } if *id == sequence.id && *data_type == DataType::Integer && !*nullable
        ));
        assert!(matches!(
            &select.columns[1].expr,
            BoundExpr::Currval {
                sequence: id,
                data_type,
                nullable
            } if *id == sequence.id && *data_type == DataType::Integer && !*nullable
        ));
        assert!(matches!(
            &select.columns[2].expr,
            BoundExpr::Setval {
                sequence: id,
                is_called: Some(_),
                data_type,
                nullable,
                ..
            } if *id == sequence.id && *data_type == DataType::Integer && !*nullable
        ));
        assert!(mutates_sequences(&bound));

        let currval_only = bind(
            &parse("select currval('users_id_seq') from users").unwrap(),
            &catalog,
        )
        .unwrap();
        assert!(!mutates_sequences(&currval_only));
    }

    #[test]
    fn binder_validates_sequence_function_and_default_arguments() {
        let catalog = catalog_with_users_and_sequence();

        let err = bind(
            &parse("select nextval('missing_seq') from users").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);

        let err = bind(
            &parse("select setval('users_id_seq', 'not-an-integer') from users").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);

        let stmt = parse("create table t (id integer primary key default nextval('users_id_seq'))")
            .unwrap();
        assert!(bind(&stmt, &catalog).is_ok());

        let err = bind(
            &parse("create table bad (id text primary key default nextval('users_id_seq'))")
                .unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);

        let err = bind(
            &parse("create table missing (id integer primary key default nextval('missing_seq'))")
                .unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_rejects_scalar_function_type_mismatch() {
        let catalog = catalog_with_users();
        let stmt = parse("select upper(id) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_rejects_scalar_function_wrong_arity() {
        let catalog = catalog_with_users();
        let stmt = parse("select upper(name, name) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);

        let stmt = parse("select format_type(NULL)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);

        let stmt = parse("select pg_get_indexdef($1, $2)").unwrap();
        let err = bind_parameterized(&stmt, &catalog, &[]).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_current_setting_wrong_shape() {
        let catalog = catalog_with_users();
        let err = bind(&parse("select current_setting()").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);

        let err = bind(&parse("select current_setting(1)").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_types_null_in_list_from_list_values() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where null in (1, 2)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            select.filter,
            Some(BoundExpr::InList { ref expr, nullable: true, .. })
                if matches!(
                    expr.as_ref(),
                    BoundExpr::Literal {
                        value: Value::Null,
                        data_type: DataType::Integer,
                        nullable: true
                    }
                )
        ));
    }

    #[test]
    fn binder_binds_scalar_subquery_in_projection() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select (select max(id) from accounts) as m from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        // A scalar subquery is always nullable (empty result is NULL), and its
        // type is the single output column's type (max(id) -> Integer).
        assert_eq!(select.output_schema[0].data_type, DataType::Integer);
        assert!(matches!(
            select.columns[0].expr,
            BoundExpr::ScalarSubquery {
                data_type: DataType::Integer,
                nullable: true,
                ..
            }
        ));
    }

    #[test]
    fn binder_rejects_multi_column_scalar_subquery() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select (select id, owner from accounts) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_binds_in_subquery() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select name from users where id in (select id from accounts)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            select.filter,
            Some(BoundExpr::InSubquery {
                negated: false,
                data_type: DataType::Boolean,
                ..
            })
        ));
    }

    #[test]
    fn binder_rejects_in_subquery_type_mismatch() {
        // `name` is TEXT but the subquery column `id` is INTEGER; no implicit cast.
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select id from users where name in (select id from accounts)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_rejects_multi_column_in_subquery() {
        let catalog = catalog_with_users_and_accounts();
        let stmt =
            parse("select id from users where id in (select id, owner from accounts)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_binds_exists_subquery() {
        let catalog = catalog_with_users_and_accounts();
        // EXISTS ignores the projected columns and is a non-null boolean.
        let stmt =
            parse("select name from users where exists (select id, owner from accounts)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            select.filter,
            Some(BoundExpr::Exists {
                negated: false,
                data_type: DataType::Boolean,
                nullable: false,
                ..
            })
        ));
    }

    #[test]
    fn binder_binds_derived_table_columns() {
        let catalog = catalog_with_users();
        let stmt = parse("select d.x from (select id as x from users) as d").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.output_schema[0].name, "x");
        assert_eq!(select.output_schema[0].data_type, DataType::Integer);
    }

    #[test]
    fn binder_applies_derived_column_aliases() {
        let catalog = catalog_with_users();
        let stmt = parse("select d.y from (select id from users) as d(y)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.output_schema[0].name, "y");
    }

    #[test]
    fn binder_rejects_too_many_derived_column_aliases() {
        let catalog = catalog_with_users();
        let stmt = parse("select d.a from (select id from users) as d(a, b)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_binds_create_view_with_exact_column_aliases() {
        let catalog = catalog_with_users();
        let stmt =
            parse("create view v (user_id, user_name) as select id, name from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::CreateView { columns, query, .. } = bound else {
            panic!("expected CREATE VIEW");
        };
        assert_eq!(
            columns,
            vec!["user_id".to_string(), "user_name".to_string()]
        );
        assert_eq!(query.output_schema().len(), 2);
    }

    #[test]
    fn binder_rejects_create_view_column_alias_count_mismatch() {
        let catalog = catalog_with_users();
        let stmt = parse("create view v (a, b) as select id from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_duplicate_create_view_column_aliases() {
        let catalog = catalog_with_users();
        let stmt = parse("create view v (a, a) as select id, name from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_create_view_parameters() {
        let catalog = catalog_with_users();
        let stmt = parse("create view v as select $1").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn binder_accepts_composite_primary_key() {
        let catalog = catalog_with_users();
        let stmt =
            parse("create table teams (id integer, org integer, primary key (id, org))").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::CreateTable {
            if_not_exists,
            primary_key,
            ..
        } = bound
        else {
            panic!("expected CreateTable");
        };
        assert!(!if_not_exists);
        assert_eq!(primary_key, vec!["id".to_string(), "org".to_string()]);
    }

    #[test]
    fn binder_preserves_create_table_if_not_exists() {
        let catalog = catalog_with_users();
        let stmt = parse("create table if not exists teams (id integer primary key)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::CreateTable { if_not_exists, .. } = bound else {
            panic!("expected CreateTable");
        };
        assert!(if_not_exists);
    }

    #[test]
    fn binder_rejects_duplicate_primary_key_column_with_syntax_error() {
        let catalog = catalog_with_users();
        let stmt = parse("create table teams (id integer, primary key (id, id))").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_nested_aggregates() {
        let catalog = catalog_with_users();
        let stmt = parse("select sum(count(*)) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
    }

    #[test]
    fn binder_rejects_aggregates_in_insert_values() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users (id, name) values (count(*), 'Ada')").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
    }

    #[test]
    fn binder_rejects_aggregates_in_update_assignments() {
        let catalog = catalog_with_users();
        let stmt = parse("update users set name = max(name)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
    }

    #[test]
    fn binder_rejects_aggregates_in_update_filter() {
        let catalog = catalog_with_users();
        let stmt = parse("update users set name = 'Ada' where count(*) > 0").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
    }

    #[test]
    fn binder_rejects_aggregates_in_delete_filter() {
        let catalog = catalog_with_users();
        let stmt = parse("delete from users where count(*) > 0").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
    }

    #[test]
    fn binder_rejects_nullable_expression_for_non_null_assignment() {
        let catalog = catalog_with_users();
        let stmt = parse(
            "insert into users (id, name) values (case when true then null else 1 end, 'Ada')",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::NotNullViolation);
    }

    #[test]
    fn binder_rejects_insert_that_omits_non_null_column() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users (name) values ('Ada')").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::NotNullViolation);
    }

    #[test]
    fn physical_planner_uses_index_scan_for_primary_key_equality() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id = 7").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        assert!(format!("{physical:?}").contains("IndexScan"));
        assert!(format!("{physical:?}").contains("index: 0"));
    }

    #[test]
    fn physical_planner_preserves_residual_filter_on_index_scan() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id = 7 and name = 'Ada'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        let text = format!("{physical:?}");
        assert!(text.contains("IndexScan"));
        assert!(text.contains("filter: Some"));
    }

    #[test]
    fn physical_planner_uses_seq_scan_for_non_key_filter() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where name = 'Ada'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        assert!(format!("{physical:?}").contains("SeqScan"));
    }

    #[test]
    fn logical_and_physical_planners_preserve_system_scan() {
        let catalog = catalog_with_users();
        let stmt = parse("select oid from pg_catalog.pg_class where oid = 1").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();

        let LogicalPlan::Projection { source, .. } = &logical else {
            panic!("expected projection, got {logical:?}");
        };
        let LogicalPlan::SystemScan { view, filter } = source.as_ref() else {
            panic!("expected system scan, got {source:?}");
        };
        assert_eq!(*view, SystemView::PgClass);
        assert!(filter.is_some());

        let physical = physical_plan(&logical, &catalog).unwrap();
        let PhysicalPlan::Projection { source, .. } = &physical else {
            panic!("expected projection, got {physical:?}");
        };
        let PhysicalPlan::SystemScan {
            view,
            output_schema,
            filter,
        } = source.as_ref()
        else {
            panic!("expected system scan, got {source:?}");
        };
        assert_eq!(*view, SystemView::PgClass);
        assert!(output_schema.iter().any(|column| column.name == "relname"));
        assert!(filter.is_some());

        let text = format_explain(&physical);
        assert!(text.contains("SystemScan view=pg_catalog.pg_class filter=yes"));
    }

    #[test]
    fn physical_planner_uses_secondary_index_for_indexed_column() {
        let catalog = catalog_with_users_and_name_index();
        for predicate in ["name = 'Ada'", "name > 'Ada'"] {
            let stmt = parse(&format!("select id from users where {predicate}")).unwrap();
            let bound = bind(&stmt, &catalog).unwrap();
            let logical = logical_plan(&bound).unwrap();
            let physical = physical_plan(&logical, &catalog).unwrap();

            let text = format!("{physical:?}");
            assert!(
                text.contains("IndexScan"),
                "expected IndexScan for {predicate}"
            );
            // The secondary index is id 2 (the primary-key constraint index is id 1).
            assert!(
                text.contains("index: 2"),
                "expected secondary index for {predicate}"
            );
        }
    }

    #[test]
    fn physical_planner_prefers_primary_key_over_secondary_index() {
        let catalog = catalog_with_users_and_name_index();
        // Both columns are indexed; the primary key wins, and the name predicate
        // becomes the residual filter.
        let stmt = parse("select id from users where id = 7 and name = 'Ada'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        let text = format!("{physical:?}");
        assert!(text.contains("IndexScan"));
        assert!(text.contains("index: 0"));
        assert!(text.contains("filter: Some"));
    }

    #[test]
    fn format_explain_does_not_depend_on_previous_planning_cache() {
        let physical = PhysicalPlan::SeqScan {
            table: 7,
            table_name: "users".to_string(),
            filter: None,
        };

        let text = format_explain(&physical);
        assert!(text.contains("users(7)"));
    }

    #[test]
    fn physical_planner_uses_index_scan_for_text_primary_key_equality() {
        let catalog = catalog_with_text_key_table();
        let stmt = parse("select label from codes where code = 'abc'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        assert!(format!("{physical:?}").contains("IndexScan"));
    }

    #[test]
    fn explain_formats_physical_plan_tree() {
        let catalog = catalog_with_users();
        let stmt = parse("explain select name from users where id = 7").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let BoundStatement::Explain(inner) = bound else {
            panic!("expected explain");
        };
        let logical = logical_plan(&inner).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();
        let text = format_explain(&physical);

        assert!(text.contains("IndexScan"));
        assert!(text.contains("users"));
    }

    #[test]
    fn physical_planner_uses_hash_join_for_inner_equi_join() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select users.id from users join accounts on users.name = accounts.owner")
            .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        assert!(format_explain(&physical).contains("HashJoin keys=1"));
    }

    #[test]
    fn physical_planner_extracts_all_equi_keys_from_conjunction() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select users.id from users join accounts \
             on users.id = accounts.id and users.name = accounts.owner",
        )
        .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        assert!(format_explain(&physical).contains("HashJoin keys=2"));
    }

    #[test]
    fn physical_planner_uses_nested_loop_join_for_non_equi_join() {
        let catalog = catalog_with_users_and_accounts();
        let stmt =
            parse("select users.id from users join accounts on users.id < accounts.id").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        let text = format_explain(&physical);
        assert!(text.contains("NestedLoopJoin"));
        assert!(!text.contains("HashJoin"));
    }

    #[test]
    fn physical_planner_uses_nested_loop_join_for_outer_equi_join() {
        let catalog = catalog_with_users_and_accounts();
        let stmt =
            parse("select users.id from users left join accounts on users.name = accounts.owner")
                .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        let text = format_explain(&physical);
        assert!(text.contains("NestedLoopJoin"));
        assert!(!text.contains("HashJoin"));
    }

    #[test]
    fn binder_expands_wildcard_projection() {
        let catalog = catalog_with_users();
        let stmt = parse("select * from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected select");
        };

        assert_eq!(select.columns.len(), 2);
        assert_eq!(select.output_schema[0].name, "id");
        assert_eq!(select.output_schema[1].name, "name");
    }

    #[test]
    fn binder_types_count_star_as_non_null_integer() {
        let catalog = catalog_with_users();
        let stmt = parse("select count(*) as c from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0].expr,
            BoundExpr::AggregateCall {
                func: AggregateFunc::Count,
                arg: None,
                data_type: DataType::Integer,
                nullable: false,
                ..
            }
        ));
    }

    #[test]
    fn binder_treats_having_as_aggregate_context() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users having false").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::DatatypeMismatch);
        assert!(err.message.contains("GROUP BY"));
    }

    #[test]
    fn logical_planner_applies_non_aggregate_having() {
        let catalog = catalog_with_users();
        let stmt = parse("select count(*) from users having false").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let text = format!("{logical:?}");

        assert!(text.contains("Aggregate"));
        assert!(text.contains("Filter"));
    }

    #[test]
    fn binder_types_case_from_later_non_null_branch() {
        let catalog = catalog_with_users();
        let stmt =
            parse("select case when id = 1 then null else name end as display from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected select");
        };

        assert_eq!(select.output_schema[0].data_type, DataType::Text);
        assert!(matches!(
            select.columns[0].expr,
            BoundExpr::Case {
                data_type: DataType::Text,
                nullable: true,
                ..
            }
        ));
    }

    #[test]
    fn physical_planner_uses_exact_key_range_for_primary_key_equality() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id = 7").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let physical = physical_plan(&logical, &catalog).unwrap();

        assert!(matches!(
            physical,
            PhysicalPlan::Projection { source, .. }
                if matches!(
                    source.as_ref(),
                    PhysicalPlan::IndexScan {
                        range: common::KeyRange::Exact(common::Key(values)),
                        filter: None,
                        ..
                    } if values == &vec![Value::Integer(7)]
                )
        ));
    }

    #[test]
    fn binds_parameter_type_from_filter_context() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = $1").unwrap();
        let (bound, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();
        assert_eq!(params, vec![DataType::Integer]);

        let substituted = substitute_params(&bound, &[Value::Integer(7)]).unwrap();
        assert!(collect_param_types(&substituted, &[]).unwrap().is_empty());
    }

    #[test]
    fn binds_array_parameter_type_from_any_left_operand() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = ANY($1)").unwrap();
        let (bound, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();
        let array_type = DataType::Array(common::ArrayType::new(DataType::Integer).unwrap());
        assert_eq!(params, vec![array_type]);

        let array = common::SqlArray::new(
            DataType::Integer,
            vec![common::ArrayDimension::new(2, 1)],
            vec![Value::Integer(1), Value::Integer(2)],
        )
        .unwrap();
        assert!(substitute_params(&bound, &[Value::Array(array)]).is_ok());
    }

    #[test]
    fn array_constructor_infers_leading_parameter_from_later_element() {
        let catalog = catalog_with_users();
        let stmt = parse("select ARRAY[$1, 1]").unwrap();
        let (_, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();
        assert_eq!(params, vec![DataType::Integer]);

        let stmt = parse("select ARRAY[1, $1]").unwrap();
        let (_, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();
        assert_eq!(params, vec![DataType::Integer]);
    }

    #[test]
    fn binder_rejects_nested_empty_array_shape() {
        let catalog = catalog_with_users();
        let stmt = parse("select ARRAY[ARRAY[], ARRAY[]]::integer[]").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binds_insert_parameters_by_column_type() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users (id, name) values ($1, $2)").unwrap();
        let (_, params) = bind_parameterized(&stmt, &catalog, &[]).unwrap();
        assert_eq!(params, vec![DataType::Integer, DataType::Text]);
    }

    #[test]
    fn honors_declared_parameter_type() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where name = $1").unwrap();
        let (_, params) = bind_parameterized(&stmt, &catalog, &[Some(DataType::Text)]).unwrap();
        assert_eq!(params, vec![DataType::Text]);
    }

    #[test]
    fn rejects_declared_type_conflicting_with_use() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = $1").unwrap();
        let err = bind_parameterized(&stmt, &catalog, &[Some(DataType::Text)]).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn errors_when_parameter_type_cannot_be_determined() {
        let catalog = catalog_with_users();
        let stmt = parse("select $1 from users").unwrap();
        let err = bind_parameterized(&stmt, &catalog, &[]).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn rejects_parameter_number_above_maximum() {
        // PostgreSQL caps bind parameters at 65535 (the wire protocol uses a
        // 16-bit parameter count). A `$N` above that must be rejected at bind
        // time — otherwise `collect_param_types` resizes a Vec to `N` entries,
        // and an attacker-supplied `$4294967295` forces a multi-GB allocation
        // (process abort, whole-server DoS).
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = $70000").unwrap();
        let err = bind_parameterized(&stmt, &catalog, &[]).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn accepts_parameter_number_at_maximum() {
        // $65535 is the boundary (PostgreSQL's maximum) and must NOT be rejected
        // by the cap. Declared types are supplied for every position so the only
        // thing that could reject it is an off-by-one in the cap check.
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = $65535").unwrap();
        let declared = vec![Some(DataType::Integer); 65535];
        assert!(bind_parameterized(&stmt, &catalog, &declared).is_ok());
    }

    #[test]
    fn simple_bind_rejects_parameters() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = $1").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn substitute_rejects_value_of_wrong_type() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where id = $1").unwrap();
        let (bound, _) = bind_parameterized(&stmt, &catalog, &[]).unwrap();
        let err = substitute_params(&bound, &[Value::Text("x".to_string())]).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_assigns_distinct_binding_ids_for_self_join() {
        let catalog = catalog_with_users();
        let stmt = parse("select a.id from users as a join users as b on a.id = b.id").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        let Some(BoundFrom::Join { left, right, .. }) = select.from else {
            panic!("expected join");
        };
        let BoundFrom::Table {
            binding: left_binding,
            ..
        } = *left
        else {
            panic!("expected left table");
        };
        let BoundFrom::Table {
            binding: right_binding,
            ..
        } = *right
        else {
            panic!("expected right table");
        };
        assert_ne!(
            left_binding, right_binding,
            "self-join occurrences must get distinct binding ids"
        );
    }

    #[test]
    fn binder_resolves_unqualified_identifiers_case_insensitively() {
        let catalog = catalog_with_users();
        let stmt = parse("select ID from USERS where NAME = 'Ada'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.output_schema[0].name, "id");
    }

    #[test]
    fn binder_binds_create_index_as_passthrough() {
        let catalog = catalog_with_users();
        let stmt = parse("create index users_name on users (name)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        assert!(matches!(
            bound,
            BoundStatement::CreateIndex { ref name, ref table, ref columns, unique: false }
                if name == "users_name" && table == "users" && columns == &["name".to_string()]
        ));
    }

    #[test]
    fn binder_rejects_drop_of_unknown_table() {
        let catalog = catalog_with_users();
        let stmt = parse("drop table missing").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_resolves_plain_drop_table_to_table_id() {
        let catalog = catalog_with_users();
        let stmt = parse("drop table users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        assert!(matches!(
            bound,
            BoundStatement::DropTable {
                ref targets,
                if_exists: false,
            } if targets.as_slice() == [DropTableTarget {
                name: "users".to_string(),
                table: Some(1),
            }]
        ));
    }

    #[test]
    fn binder_accepts_drop_if_exists_of_unknown_table_as_noop() {
        let catalog = catalog_with_users();
        let stmt = parse("drop table if exists missing").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        assert!(matches!(
            bound,
            BoundStatement::DropTable {
                ref targets,
                if_exists: true,
            } if targets.as_slice() == [DropTableTarget {
                name: "missing".to_string(),
                table: None,
            }]
        ));
    }

    #[test]
    fn binder_preserves_multi_table_drop_targets() {
        let catalog = catalog_with_users();
        let orders = catalog
            .create_table(
                "orders".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                }],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();

        let bound = bind(&parse("drop table users, orders").unwrap(), &catalog).unwrap();
        let BoundStatement::DropTable {
            targets,
            if_exists: false,
        } = bound
        else {
            panic!("expected DropTable");
        };
        assert_eq!(
            targets,
            vec![
                DropTableTarget {
                    name: "users".to_string(),
                    table: Some(1),
                },
                DropTableTarget {
                    name: "orders".to_string(),
                    table: Some(orders.id),
                },
            ]
        );
    }

    #[test]
    fn binder_rejects_drop_of_unknown_index() {
        let catalog = catalog_with_users();
        let stmt = parse("drop index missing").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_accepts_table_without_primary_key() {
        let catalog = MemoryCatalog::empty();
        let stmt = parse("create table t (a integer not null)").unwrap();
        let BoundStatement::CreateTable { primary_key, .. } = bind(&stmt, &catalog).unwrap() else {
            panic!("expected CREATE TABLE");
        };
        assert!(primary_key.is_empty());
    }

    #[test]
    fn binder_binds_between_predicate() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id between 1 and 10").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(select.filter, Some(BoundExpr::Between { .. })));
    }

    #[test]
    fn binder_binds_like_predicate() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where name like 'A%'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(select.filter, Some(BoundExpr::Like { .. })));
    }

    #[test]
    fn binder_desugars_coalesce_to_case_with_tight_nullability() {
        let catalog = catalog_with_users();
        let stmt = parse("select coalesce(name, 'fallback') from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        let expr = &select.columns[0].expr;
        assert!(matches!(expr, BoundExpr::Case { .. }));
        assert_eq!(expr.data_type(), DataType::Text);
        // A non-null fallback makes the whole COALESCE non-nullable.
        assert!(!expr.nullable());
    }

    #[test]
    fn binder_coalesce_all_nullable_stays_nullable() {
        let catalog = catalog_with_users();
        let stmt = parse("select coalesce(name, name) from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(select.columns[0].expr.nullable());
    }

    #[test]
    fn binder_coalesce_rejects_type_mismatch() {
        let catalog = catalog_with_users();
        let stmt = parse("select coalesce(name, 1) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_nullif_is_nullable_and_typed_from_first_arg() {
        let catalog = catalog_with_users();
        let stmt = parse("select nullif(id, 0) from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        let expr = &select.columns[0].expr;
        assert_eq!(expr.data_type(), DataType::Integer);
        assert!(expr.nullable());
    }

    #[test]
    fn binder_is_distinct_from_is_never_null() {
        let catalog = catalog_with_users();
        let stmt = parse("select id is distinct from 1 from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        let expr = &select.columns[0].expr;
        assert_eq!(expr.data_type(), DataType::Boolean);
        assert!(!expr.nullable());
    }

    #[test]
    fn binder_assigns_math_function_result_types() {
        let catalog = catalog_with_users();
        let cases = [
            ("select abs(id) from users", DataType::Integer),
            ("select floor(id) from users", DataType::Integer),
            ("select floor(2.5) from users", DataType::Double),
            ("select round(2.5) from users", DataType::Double),
            ("select sqrt(id) from users", DataType::Double),
            ("select power(id, 2) from users", DataType::Double),
            ("select mod(id, 2) from users", DataType::Integer),
        ];
        for (sql, expected) in cases {
            let bound = bind(&parse(sql).unwrap(), &catalog).unwrap();
            let BoundStatement::Query(BoundQuery {
                body: BoundQueryBody::Select(select),
                ..
            }) = bound
            else {
                panic!("expected bound select for {sql}");
            };
            assert_eq!(select.columns[0].expr.data_type(), expected, "for `{sql}`");
        }

        // MOD is integer-only; a double argument is a type mismatch.
        let err = bind(&parse("select mod(2.5, 1.0) from users").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_assigns_string_function_result_types() {
        let catalog = catalog_with_users();

        // CONCAT never returns NULL, even over a nullable argument.
        let bound = bind(
            &parse("select concat(name, '!') from users").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.columns[0].expr.data_type(), DataType::Text);
        assert!(!select.columns[0].expr.nullable());

        let bound = bind(
            &parse("select concat(null, null, null, null, null) from users").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.columns[0].expr.data_type(), DataType::Text);
        assert!(!select.columns[0].expr.nullable());

        let cases = [
            ("select replace(name, 'a', 'b') from users", DataType::Text),
            ("select position('a' in name) from users", DataType::Integer),
            ("select left(name, 2) from users", DataType::Text),
            ("select right(name, 2) from users", DataType::Text),
        ];
        for (sql, expected) in cases {
            let bound = bind(&parse(sql).unwrap(), &catalog).unwrap();
            let BoundStatement::Query(BoundQuery {
                body: BoundQueryBody::Select(select),
                ..
            }) = bound
            else {
                panic!("expected bound select for {sql}");
            };
            assert_eq!(select.columns[0].expr.data_type(), expected, "for `{sql}`");
        }

        // LEFT requires an integer count.
        let err = bind(
            &parse("select left(name, 'x') from users").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_assigns_statistical_aggregate_types() {
        let catalog = catalog_with_users();

        // STDDEV/VARIANCE accept a numeric argument and return nullable DOUBLE.
        for sql in [
            "select stddev(id) from users",
            "select stddev_pop(id) from users",
            "select var_samp(id) from users",
            "select var_pop(id) from users",
            "select variance(id) from users",
        ] {
            let bound = bind(&parse(sql).unwrap(), &catalog).unwrap();
            let BoundStatement::Query(BoundQuery {
                body: BoundQueryBody::Select(select),
                ..
            }) = bound
            else {
                panic!("expected bound select for {sql}");
            };
            assert_eq!(
                select.columns[0].expr.data_type(),
                DataType::Double,
                "for `{sql}`"
            );
            assert!(select.columns[0].expr.nullable());
        }

        // BOOL_AND/BOOL_OR require a boolean argument and return BOOLEAN.
        let bound = bind(
            &parse("select bool_and(id = 1) from users").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.columns[0].expr.data_type(), DataType::Boolean);

        // STDDEV rejects non-numeric arguments; BOOL_AND rejects non-boolean ones.
        let err = bind(&parse("select stddev(name) from users").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
        let err = bind(&parse("select bool_and(id) from users").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_extract_returns_double_and_validates_source() {
        let catalog = catalog_with_users();

        let bound = bind(
            &parse("select extract(year from date '2024-03-15') from users").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert_eq!(select.columns[0].expr.data_type(), DataType::Double);

        // EXTRACT requires a date/timestamp source.
        let err = bind(
            &parse("select extract(year from id) from users").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_binds_statement_timestamp_functions_as_timestamptz() {
        let catalog = catalog_with_users();
        let bound = bind(&parse("select current_timestamp, now()").unwrap(), &catalog).unwrap();
        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        for item in &select.columns {
            assert_eq!(item.expr.data_type(), DataType::TimestampTz);
            assert!(!item.expr.nullable());
        }

        let err = bind(&parse("select current_date").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_casts_timestamptz_expression_assignments_to_timestamp_columns() {
        let catalog = catalog_with_temporal_columns();

        let bound = bind(
            &parse("insert into t (id, ts) values (1, current_timestamp)").unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Insert {
            source: BoundInsertSource::Values { rows, .. },
            ..
        } = bound
        else {
            panic!("expected INSERT VALUES");
        };
        assert_timestamptz_to_timestamp_assignment_cast(&rows[0][1]);

        let bound = bind(&parse("update t set ts = now()").unwrap(), &catalog).unwrap();
        let BoundStatement::Update { assignments, .. } = bound else {
            panic!("expected UPDATE");
        };
        assert_timestamptz_to_timestamp_assignment_cast(&assignments[0].1);

        let bound = bind(
            &parse(
                "insert into t (id, tstz) values (1, current_timestamp) \
                 on conflict (id) do update set ts = excluded.tstz",
            )
            .unwrap(),
            &catalog,
        )
        .unwrap();
        let BoundStatement::Insert {
            on_conflict: Some(BoundOnConflict::DoUpdate { assignments, .. }),
            ..
        } = bound
        else {
            panic!("expected ON CONFLICT DO UPDATE");
        };
        assert_timestamptz_to_timestamp_assignment_cast(&assignments[0].1);

        let err = bind(
            &parse("insert into t (id, ts) select id, tstz from t").unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_desugars_comma_from_list_into_cross_join() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select users.id from users, accounts").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Query(BoundQuery {
            body: BoundQueryBody::Select(select),
            ..
        }) = bound
        else {
            panic!("expected bound select");
        };
        assert!(matches!(
            select.from,
            Some(BoundFrom::Join {
                join_type: JoinType::Cross,
                condition: None,
                ..
            })
        ));
    }

    #[test]
    fn parser_rejects_cross_join_with_on_predicate() {
        // `CROSS JOIN ... ON` is rejected at parse time (not by the binder), with
        // a SyntaxError; the statement is still rejected end-to-end.
        let err = parse("select users.id from users cross join accounts on users.id = accounts.id")
            .unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    fn first_index_scan_range(plan: &PhysicalPlan) -> Option<&common::KeyRange> {
        match plan {
            PhysicalPlan::IndexScan { range, .. } => Some(range),
            PhysicalPlan::Projection { source, .. }
            | PhysicalPlan::Filter { source, .. }
            | PhysicalPlan::Sort { source, .. }
            | PhysicalPlan::Limit { source, .. } => first_index_scan_range(source),
            _ => None,
        }
    }

    fn hash_join_keys(plan: &PhysicalPlan) -> Option<(&[usize], &[usize])> {
        match plan {
            PhysicalPlan::HashJoin {
                left_keys,
                right_keys,
                ..
            } => Some((left_keys, right_keys)),
            PhysicalPlan::Projection { source, .. }
            | PhysicalPlan::Filter { source, .. }
            | PhysicalPlan::Sort { source, .. }
            | PhysicalPlan::Limit { source, .. } => hash_join_keys(source),
            _ => None,
        }
    }

    fn plan_of(catalog: &MemoryCatalog, sql: &str) -> PhysicalPlan {
        let stmt = parse(sql).unwrap();
        let bound = bind(&stmt, catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        physical_plan(&logical, catalog).unwrap()
    }

    #[test]
    fn physical_planner_builds_exclusive_lower_bound_range() {
        use common::{Key, KeyRange};
        use std::ops::Bound;
        let catalog = catalog_with_users();
        let physical = plan_of(&catalog, "select name from users where id > 7");
        assert_eq!(
            first_index_scan_range(&physical),
            Some(&KeyRange::Range {
                start: Bound::Excluded(Key(vec![Value::Integer(7)])),
                end: Bound::Unbounded,
            })
        );
    }

    #[test]
    fn physical_planner_builds_inclusive_lower_bound_range() {
        use common::{Key, KeyRange};
        use std::ops::Bound;
        let catalog = catalog_with_users();
        let physical = plan_of(&catalog, "select name from users where id >= 7");
        assert_eq!(
            first_index_scan_range(&physical),
            Some(&KeyRange::Range {
                start: Bound::Included(Key(vec![Value::Integer(7)])),
                end: Bound::Unbounded,
            })
        );
    }

    #[test]
    fn physical_planner_builds_exclusive_upper_bound_range() {
        use common::{Key, KeyRange};
        use std::ops::Bound;
        let catalog = catalog_with_users();
        let physical = plan_of(&catalog, "select name from users where id < 7");
        assert_eq!(
            first_index_scan_range(&physical),
            Some(&KeyRange::Range {
                start: Bound::Unbounded,
                end: Bound::Excluded(Key(vec![Value::Integer(7)])),
            })
        );
    }

    #[test]
    fn physical_planner_builds_inclusive_upper_bound_range() {
        use common::{Key, KeyRange};
        use std::ops::Bound;
        let catalog = catalog_with_users();
        let physical = plan_of(&catalog, "select name from users where id <= 7");
        assert_eq!(
            first_index_scan_range(&physical),
            Some(&KeyRange::Range {
                start: Bound::Unbounded,
                end: Bound::Included(Key(vec![Value::Integer(7)])),
            })
        );
    }

    #[test]
    fn physical_planner_rebases_single_hash_join_key() {
        let catalog = catalog_with_users_and_accounts();
        let physical = plan_of(
            &catalog,
            "select users.id from users join accounts on users.id = accounts.id",
        );
        // users = (id, name) -> left width 2; accounts.id is global slot 2.
        // left key slot 0 (users.id), right key rebased to 0 (2 - 2).
        assert_eq!(
            hash_join_keys(&physical),
            Some((&[0usize][..], &[0usize][..]))
        );
    }

    #[test]
    fn physical_planner_rebases_multiple_hash_join_keys() {
        let catalog = catalog_with_users_and_accounts();
        let physical = plan_of(
            &catalog,
            "select users.id from users join accounts \
             on users.id = accounts.id and users.name = accounts.owner",
        );
        // left keys = [id=0, name=1]; right keys rebased = [id 2-2=0, owner 3-2=1].
        assert_eq!(
            hash_join_keys(&physical),
            Some((&[0usize, 1][..], &[0usize, 1][..]))
        );
    }

    #[test]
    fn logical_planner_extracts_nested_aggregate_under_scalar_function() {
        let catalog = catalog_with_users();
        let stmt = parse("select abs(sum(id)) from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();

        let LogicalPlan::Projection {
            expressions,
            source,
            ..
        } = &logical
        else {
            panic!("expected projection, got {logical:?}");
        };
        // The scalar ABS keeps its Function shape; its argument is the extracted
        // aggregate, rewritten to a LocalRef into the Aggregate output row.
        assert!(matches!(
            expressions.as_slice(),
            [BoundExpr::Function { args, .. }]
                if matches!(args.as_slice(), [BoundExpr::LocalRef { slot: 0, .. }])
        ));
        let LogicalPlan::Aggregate { aggregates, .. } = source.as_ref() else {
            panic!("expected aggregate source, got {source:?}");
        };
        assert_eq!(aggregates.len(), 1);
        assert!(matches!(aggregates[0].func, AggregateFunc::Sum));
    }

    #[test]
    fn logical_planner_builds_limit_with_offset_none() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users limit 5").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();

        assert!(matches!(
            logical,
            LogicalPlan::Limit {
                count: 5,
                offset: None,
                ..
            }
        ));
    }

    #[test]
    fn logical_planner_models_bare_offset_as_unbounded_limit() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users offset 3").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();

        assert!(matches!(
            logical,
            LogicalPlan::Limit {
                count: u64::MAX,
                offset: Some(3),
                ..
            }
        ));
    }

    #[test]
    fn logical_planner_orders_sort_under_projection_under_limit() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users order by id limit 2").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();

        let LogicalPlan::Limit { source, .. } = &logical else {
            panic!("expected limit, got {logical:?}");
        };
        let LogicalPlan::Projection { source, .. } = source.as_ref() else {
            panic!("expected projection under limit, got {source:?}");
        };
        assert!(
            matches!(source.as_ref(), LogicalPlan::Sort { .. }),
            "expected sort under projection, got {source:?}"
        );
    }

    #[test]
    fn simplify_folds_constant_arithmetic_in_filter() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id = 3 + 4").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        // The folded `id = 7` makes the primary-key index usable as an exact match.
        let physical = physical_plan(&logical, &catalog).unwrap();
        let text = format!("{physical:?}");
        assert!(text.contains("IndexScan"), "expected IndexScan, got {text}");
        assert!(text.contains("Exact"), "expected exact key, got {text}");
    }

    #[test]
    fn simplify_does_not_fold_division_by_zero() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id = 1 / 0").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        // `1 / 0` must survive so the executor raises the runtime error; the
        // comparand is not a bare literal, so the plan stays a SeqScan.
        let physical = physical_plan(&logical, &catalog).unwrap();
        assert!(format!("{physical:?}").contains("SeqScan"));
    }

    #[test]
    fn simplify_drops_constant_true_filter_above_join() {
        let catalog = catalog_with_users_and_accounts();
        // `1 = 1` folds to TRUE; the residual Filter above the cross join is removed.
        let stmt = parse("select users.id from users, accounts where 1 = 1").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        assert!(
            !format!("{logical:?}").contains("Filter"),
            "constant-true filter should be removed: {logical:?}"
        );
    }

    #[test]
    fn simplify_drops_redundant_and_true_in_filter() {
        let catalog = catalog_with_users();
        // `id = 7 AND true` simplifies to `id = 7` (dropping only the TRUE literal),
        // which keeps the predicate usable as an index key.
        let stmt = parse("select name from users where id = 7 and true").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let LogicalPlan::Projection { source, .. } = &logical else {
            panic!("expected projection, got {logical:?}");
        };
        assert!(
            matches!(
                source.as_ref(),
                LogicalPlan::Scan {
                    filter: Some(BoundExpr::BinaryOp { op: BinOp::Eq, .. }),
                    ..
                }
            ),
            "expected `id = 7` after dropping `AND true`, got {source:?}"
        );
    }

    #[test]
    fn simplify_keeps_fallible_operand_in_false_conjunction() {
        let catalog = catalog_with_users();
        // The executor evaluates both AND operands eagerly, so folding `false AND x`
        // must NOT discard `x` when `x` can raise at runtime (`id / 0` here). The
        // conjunction is preserved so the division-by-zero error still surfaces.
        let stmt = parse("select name from users where false and id / 0 = 1").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        let LogicalPlan::Projection { source, .. } = &logical else {
            panic!("expected projection, got {logical:?}");
        };
        assert!(
            matches!(
                source.as_ref(),
                LogicalPlan::Scan {
                    filter: Some(BoundExpr::BinaryOp { op: BinOp::And, .. }),
                    ..
                }
            ),
            "fallible operand must be preserved (no collapse to constant), got {source:?}"
        );
    }

    #[test]
    fn physical_planner_hashes_equi_keys_and_filters_residual() {
        let catalog = catalog_with_users_and_accounts();
        let physical = plan_of(
            &catalog,
            "select users.id from users join accounts \
             on users.id = accounts.id and users.id < accounts.id",
        );
        let text = format_explain(&physical);
        assert!(text.contains("HashJoin keys=1"), "got: {text}");
        assert!(
            text.contains("Filter"),
            "expected residual filter, got: {text}"
        );
    }

    #[test]
    fn physical_planner_fuses_two_sided_range_on_primary_key() {
        use common::{Key, KeyRange};
        use std::ops::Bound;
        let catalog = catalog_with_users();
        let physical = plan_of(&catalog, "select name from users where id > 5 and id < 10");
        assert_eq!(
            first_index_scan_range(&physical),
            Some(&KeyRange::Range {
                start: Bound::Excluded(Key(vec![Value::Integer(5)])),
                end: Bound::Excluded(Key(vec![Value::Integer(10)])),
            })
        );
        // Both conjuncts are consumed by the range, so no residual filter remains.
        let PhysicalPlan::Projection { source, .. } = &physical else {
            panic!("expected projection, got {physical:?}");
        };
        assert!(
            matches!(
                source.as_ref(),
                PhysicalPlan::IndexScan { filter: None, .. }
            ),
            "expected no residual filter, got {source:?}"
        );
    }

    #[test]
    fn binder_binds_copy_from_stdin_all_columns() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();
        let all_columns: Vec<_> = table.columns.iter().map(|column| column.id).collect();

        assert_eq!(
            bind(&parse("copy users from stdin").unwrap(), &catalog).unwrap(),
            BoundStatement::Copy {
                table: table.id,
                table_schema: table.clone(),
                columns: all_columns,
                direction: CopyDirection::From,
                options: CopyOptions::defaults_for(CopyFormat::Text),
                default_exprs: vec![],
                check_exprs: vec![],
            }
        );
    }

    #[test]
    fn binder_binds_copy_to_stdout_subset_csv() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();
        let name_id = table
            .columns
            .iter()
            .find(|column| column.name == "name")
            .unwrap()
            .id;

        let BoundStatement::Copy {
            table: table_id,
            columns,
            direction,
            options,
            ..
        } = bind(
            &parse("copy users (name) to stdout with (format csv, header true)").unwrap(),
            &catalog,
        )
        .unwrap()
        else {
            panic!("expected COPY");
        };
        assert_eq!(table_id, table.id);
        assert_eq!(columns, vec![name_id]);
        assert_eq!(direction, CopyDirection::To);
        assert_eq!(options.format, CopyFormat::Csv);
        assert!(options.header);
    }

    #[test]
    fn binder_rejects_copy_unknown_table() {
        let catalog = catalog_with_users();
        let err = bind(&parse("copy nope from stdin").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_rejects_copy_unknown_column() {
        let catalog = catalog_with_users();
        let err = bind(&parse("copy users (bogus) to stdout").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn binder_rejects_copy_duplicate_column() {
        let catalog = catalog_with_users();
        let err = bind(&parse("copy users (id, id) from stdin").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    // --- correlated subquery binding (docs/specs/subqueries.md, milestone S1) ---

    fn bound_query_of(stmt: BoundStatement) -> BoundQuery {
        match stmt {
            BoundStatement::Query(query) => query,
            other => panic!("expected a bound query, got {other:?}"),
        }
    }

    fn select_of(query: &BoundQuery) -> &BoundSelect {
        match &query.body {
            BoundQueryBody::Select(select) => select,
            other => panic!("expected a SELECT body, got {other:?}"),
        }
    }

    /// The subquery carried by an `Exists` filter expression.
    fn exists_query(filter: &BoundExpr) -> &BoundQuery {
        match filter {
            BoundExpr::Exists { query, .. } => query,
            other => panic!("expected EXISTS, got {other:?}"),
        }
    }

    #[test]
    fn correlated_exists_records_depth_one_correlation() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select id from users where exists \
             (select 1 from accounts where accounts.owner = users.name)",
        )
        .unwrap();
        let query = bound_query_of(bind(&stmt, &catalog).unwrap());
        assert!(query.correlations.is_empty());

        let outer_select = select_of(&query);
        let sub = exists_query(outer_select.filter.as_ref().unwrap());
        assert_eq!(sub.correlations.len(), 1);
        let entry = &sub.correlations[0];
        assert_eq!(entry.data_type, DataType::Text);
        assert!(entry.nullable);
        // The entry's outer expression is users.name in the outer scope's terms.
        assert!(
            matches!(entry.outer, BoundExpr::InputRef { slot: 1, .. }),
            "expected InputRef to users.name, got {:?}",
            entry.outer
        );
        // The body's filter compares accounts.owner to OuterRef slot 0.
        let body_filter = select_of(sub).filter.as_ref().unwrap();
        let BoundExpr::BinaryOp { right, .. } = body_filter else {
            panic!("expected comparison, got {body_filter:?}");
        };
        assert!(
            matches!(**right, BoundExpr::OuterRef { slot: 0, .. }),
            "expected OuterRef slot 0, got {right:?}"
        );
    }

    #[test]
    fn inner_binding_shadows_outer_column() {
        let catalog = catalog_with_users_and_accounts();
        // `name` exists in the inner `users u` scope, so it binds there — not
        // as a correlation to the outer users.
        let stmt = parse(
            "select id from users where exists \
             (select 1 from users u where name = 'x')",
        )
        .unwrap();
        let query = bound_query_of(bind(&stmt, &catalog).unwrap());
        let sub = exists_query(select_of(&query).filter.as_ref().unwrap());
        assert!(sub.correlations.is_empty());
        let body_filter = select_of(sub).filter.as_ref().unwrap();
        let BoundExpr::BinaryOp { left, .. } = body_filter else {
            panic!("expected comparison, got {body_filter:?}");
        };
        assert!(matches!(**left, BoundExpr::InputRef { .. }));
    }

    #[test]
    fn repeated_outer_references_share_one_slot() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select id from users where exists \
             (select 1 from accounts where owner = users.name and owner <> users.name)",
        )
        .unwrap();
        let query = bound_query_of(bind(&stmt, &catalog).unwrap());
        let sub = exists_query(select_of(&query).filter.as_ref().unwrap());
        assert_eq!(sub.correlations.len(), 1);
    }

    #[test]
    fn depth_two_correlation_chains_through_middle_scope() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select id from users where exists \
             (select 1 from accounts where exists \
              (select 1 from accounts a2 where a2.owner = users.name))",
        )
        .unwrap();
        let query = bound_query_of(bind(&stmt, &catalog).unwrap());

        // The middle subquery became correlated through re-interning: its own
        // entry is the outer users.name.
        let middle = exists_query(select_of(&query).filter.as_ref().unwrap());
        assert_eq!(middle.correlations.len(), 1);
        assert!(matches!(
            middle.correlations[0].outer,
            BoundExpr::InputRef { slot: 1, .. }
        ));

        // The innermost subquery's entry chains via OuterRef into the middle's
        // correlation list.
        let inner = exists_query(select_of(middle).filter.as_ref().unwrap());
        assert_eq!(inner.correlations.len(), 1);
        assert!(
            matches!(
                inner.correlations[0].outer,
                BoundExpr::OuterRef { slot: 0, .. }
            ),
            "expected chained OuterRef, got {:?}",
            inner.correlations[0].outer
        );
    }

    #[test]
    fn outer_reference_ambiguous_across_outer_bindings() {
        let catalog = catalog_with_users_and_accounts();
        // Both u1.name and u2.name match in the (single) outer scope.
        let stmt = parse(
            "select u1.id from users u1, users u2 where exists \
             (select 1 from accounts where owner = name)",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
        assert!(err.message.contains("ambiguous"), "{}", err.message);
    }

    #[test]
    fn set_op_arm_outer_reference_rejected() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select id from users where exists \
             (select owner from accounts where owner = users.name \
              union select owner from accounts)",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(err.message.contains("set-operation arm"), "{}", err.message);
    }

    #[test]
    fn derived_table_outer_reference_rejected() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select id from users where exists \
             (select 1 from (select owner from accounts where accounts.owner = users.name) d)",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(err.message.contains("derived table"), "{}", err.message);
    }

    #[test]
    fn values_list_outer_reference_rejected() {
        let catalog = catalog_with_users_and_accounts();
        let stmt =
            parse("select id from users where name in (values ('a'), (users.name))").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(err.message.contains("VALUES list"), "{}", err.message);
    }

    #[test]
    fn cte_body_outer_reference_fails_name_resolution() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select id from users where exists \
             (with x as (select owner from accounts where accounts.owner = users.name) \
              select 1 from x)",
        )
        .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn correlated_having_requires_grouped_outer_column() {
        let catalog = catalog_with_users_and_accounts();
        // Correlating on the grouped column is fine.
        let grouped = parse(
            "select name from users group by name having exists \
             (select 1 from accounts where owner = users.name)",
        )
        .unwrap();
        bind(&grouped, &catalog).unwrap();

        // Correlating on an ungrouped column violates the grouped rule.
        let ungrouped = parse(
            "select name from users group by name having exists \
             (select 1 from accounts where accounts.id = users.id)",
        )
        .unwrap();
        let err = bind(&ungrouped, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn grouped_correlation_rewrites_to_post_aggregate_slot() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse(
            "select name from users group by name having exists \
             (select 1 from accounts where owner = users.name)",
        )
        .unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        // The logical plan rewrites the correlation entry from InputRef
        // (pre-aggregate) to LocalRef slot 0 (the first GROUP BY column of the
        // post-aggregate row).
        let plan = logical_plan(&bound).unwrap();
        let having = find_having_predicate(&plan).expect("plan should contain the HAVING filter");
        let BoundExpr::Exists { query, .. } = having else {
            panic!("expected EXISTS in HAVING, got {having:?}");
        };
        assert!(
            matches!(
                query.correlations[0].outer,
                BoundExpr::LocalRef { slot: 0, .. }
            ),
            "expected the correlation entry rewritten to LocalRef slot 0, got {:?}",
            query.correlations[0].outer
        );
    }

    /// The predicate of the first `Filter` above an `Aggregate` (the HAVING
    /// filter) anywhere in the plan tree.
    fn find_having_predicate(plan: &LogicalPlan) -> Option<&BoundExpr> {
        match plan {
            LogicalPlan::Filter { source, predicate } => {
                if matches!(**source, LogicalPlan::Aggregate { .. }) {
                    Some(predicate)
                } else {
                    find_having_predicate(source)
                }
            }
            LogicalPlan::Projection { source, .. }
            | LogicalPlan::Sort { source, .. }
            | LogicalPlan::Limit { source, .. }
            | LogicalPlan::Distinct { source, .. }
            | LogicalPlan::Aggregate { source, .. } => find_having_predicate(source),
            _ => None,
        }
    }

    #[test]
    fn aggregate_over_only_outer_columns_rejected() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select id from users where exists (select sum(users.id) from accounts)")
            .unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(
            err.message.contains("outer-level columns"),
            "{}",
            err.message
        );
    }

    #[test]
    fn uncorrelated_subquery_has_empty_correlations() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select id from users where exists (select 1 from accounts)").unwrap();
        let query = bound_query_of(bind(&stmt, &catalog).unwrap());
        let sub = exists_query(select_of(&query).filter.as_ref().unwrap());
        assert!(sub.correlations.is_empty());
    }

    // --- Apply hoisting (docs/specs/subqueries.md §5, milestone S2) ---

    fn plan_sql(sql: &str, catalog: &MemoryCatalog) -> PhysicalPlan {
        let bound = bind(&parse(sql).unwrap(), catalog).unwrap();
        let logical = logical_plan(&bound).unwrap();
        physical_plan(&logical, catalog).unwrap()
    }

    #[test]
    fn equality_correlated_exists_decorrelates_to_hash_semi_join() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select id from users where exists \
             (select 1 from accounts where accounts.owner = users.name)",
            &catalog,
        );
        // The EXISTS conjunct is consumed entirely by the semi join: no
        // Filter, no Apply, output slots unchanged for the projection.
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection at top, got {plan:?}");
        };
        let PhysicalPlan::HashJoin {
            join_type: JoinType::Semi,
            left,
            right,
            left_keys,
            right_keys,
            ..
        } = *source
        else {
            panic!("expected hash semi join, got {source:?}");
        };
        assert_eq!(
            (left_keys.as_slice(), right_keys.as_slice()),
            (&[1][..], &[1][..])
        );
        assert!(matches!(*left, PhysicalPlan::SeqScan { filter: None, .. }));
        assert!(matches!(*right, PhysicalPlan::SeqScan { filter: None, .. }));
    }

    #[test]
    fn not_exists_with_body_filter_decorrelates_to_anti_join() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select id from users where not exists \
             (select 1 from accounts where accounts.owner = users.name and accounts.id > 5)",
            &catalog,
        );
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection, got {plan:?}");
        };
        let PhysicalPlan::HashJoin {
            join_type: JoinType::Anti,
            right,
            ..
        } = *source
        else {
            panic!("expected hash anti join, got {source:?}");
        };
        // The uncorrelated body conjunct stays on the inner scan — here as a
        // primary-key range, so the scan plans as an IndexScan.
        assert!(
            matches!(*right, PhysicalPlan::IndexScan { .. }),
            "got {right:?}"
        );
    }

    #[test]
    fn non_equality_correlation_hoists_apply_above_scan() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select id from users where exists \
             (select 1 from accounts where accounts.owner > users.name)",
            &catalog,
        );
        // Not decorrelatable: Projection over Filter over Apply over the
        // unfiltered scan; the predicate references the appended column
        // (users has 2 columns, so the Apply column is slot 2).
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection at top, got {plan:?}");
        };
        let PhysicalPlan::Filter { source, predicate } = *source else {
            panic!("expected Filter, got {source:?}");
        };
        assert!(
            matches!(predicate, BoundExpr::LocalRef { slot: 2, .. }),
            "predicate should reference the appended column, got {predicate:?}"
        );
        let PhysicalPlan::Apply {
            input,
            kind: ApplyKind::Exists { negated: false },
            correlations,
            ..
        } = *source
        else {
            panic!("expected Apply(Exists), got {source:?}");
        };
        assert_eq!(correlations.len(), 1);
        assert!(matches!(*input, PhysicalPlan::SeqScan { filter: None, .. }));
    }

    #[test]
    fn uncorrelated_in_with_column_operand_decorrelates_to_semi_join() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select name from users where id in (select id from accounts)",
            &catalog,
        );
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection, got {plan:?}");
        };
        assert!(
            matches!(
                *source,
                PhysicalPlan::HashJoin {
                    join_type: JoinType::Semi,
                    ..
                }
            ),
            "expected hash semi join, got {source:?}"
        );
    }

    #[test]
    fn not_in_decorrelates_only_when_null_free() {
        let catalog = catalog_with_users_and_accounts();
        // users.id and accounts.id are both primary keys (non-nullable):
        // NOT IN is an anti join.
        let plan = plan_sql(
            "select name from users where id not in (select id from accounts)",
            &catalog,
        );
        assert!(
            format!("{plan:?}").contains("Anti"),
            "expected anti join, got {plan:?}"
        );
        // accounts.owner is nullable: NOT IN must keep three-valued
        // semantics via the pre-pass InList (no join, no Apply).
        let plan = plan_sql(
            "select name from users where name not in (select owner from accounts)",
            &catalog,
        );
        let plan_text = format!("{plan:?}");
        assert!(
            !plan_text.contains("Anti") && !plan_text.contains("Apply"),
            "nullable NOT IN must stay a subquery expression, got {plan:?}"
        );
    }

    #[test]
    fn exists_with_aggregate_body_falls_back_to_apply() {
        let catalog = catalog_with_users_and_accounts();
        // An implicitly-aggregated body (`select max(...)`) produces exactly
        // one row no matter what the correlation filter matches, so EXISTS
        // over it is constant-true: it must NOT decorrelate to a semi join.
        let plan = plan_sql(
            "select id from users where exists \
             (select max(id) from accounts where accounts.owner = users.name)",
            &catalog,
        );
        let plan_text = format!("{plan:?}");
        assert!(
            plan_text.contains("Apply") && !plan_text.contains("Semi"),
            "expected Apply fallback, got {plan:?}"
        );
    }

    #[test]
    fn exists_with_disqualifying_body_falls_back_to_apply() {
        let catalog = catalog_with_users_and_accounts();
        // A LIMIT in the body disqualifies decorrelation.
        let plan = plan_sql(
            "select id from users where exists \
             (select 1 from accounts where accounts.owner = users.name limit 1)",
            &catalog,
        );
        assert!(
            format!("{plan:?}").contains("Apply"),
            "expected Apply fallback, got {plan:?}"
        );
        // An EXISTS under OR is not a top-level conjunct.
        let plan = plan_sql(
            "select id from users where id = 1 or exists \
             (select 1 from accounts where accounts.owner = users.name)",
            &catalog,
        );
        assert!(
            format!("{plan:?}").contains("Apply"),
            "expected Apply fallback, got {plan:?}"
        );
    }

    #[test]
    fn select_list_correlation_hoists_apply_below_projection() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select (select max(a.id) from accounts a where a.owner = users.name) from users",
            &catalog,
        );
        let PhysicalPlan::Projection {
            source,
            expressions,
            ..
        } = plan
        else {
            panic!("expected Projection, got {plan:?}");
        };
        assert!(
            matches!(expressions[0], BoundExpr::LocalRef { slot: 2, .. }),
            "projection should reference the appended column, got {:?}",
            expressions[0]
        );
        assert!(matches!(
            *source,
            PhysicalPlan::Apply {
                kind: ApplyKind::Scalar { .. },
                ..
            }
        ));
    }

    #[test]
    fn having_equality_correlation_decorrelates_above_aggregate() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select name from users group by name having exists \
             (select 1 from accounts where accounts.owner = users.name)",
            &catalog,
        );
        // The correlation entry was rewritten to a post-aggregate LocalRef,
        // which the equi-key extraction accepts: hash semi join above the
        // Aggregate, HAVING filter fully consumed.
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection, got {plan:?}");
        };
        let PhysicalPlan::HashJoin {
            join_type: JoinType::Semi,
            left,
            ..
        } = *source
        else {
            panic!("expected hash semi join, got {source:?}");
        };
        assert!(matches!(*left, PhysicalPlan::Aggregate { .. }));
    }

    #[test]
    fn having_non_equality_correlation_hoists_apply_above_aggregate() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select name from users group by name having exists \
             (select 1 from accounts where accounts.owner > users.name)",
            &catalog,
        );
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection, got {plan:?}");
        };
        let PhysicalPlan::Filter { source, .. } = *source else {
            panic!("expected HAVING Filter, got {source:?}");
        };
        let PhysicalPlan::Apply { input, .. } = *source else {
            panic!("expected Apply above the Aggregate, got {source:?}");
        };
        assert!(matches!(*input, PhysicalPlan::Aggregate { .. }));
    }

    #[test]
    fn update_with_decorrelated_where_needs_no_shape_restore() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "update users set name = 'x' where exists \
             (select 1 from accounts where accounts.owner = users.name)",
            &catalog,
        );
        let PhysicalPlan::Update { source, .. } = plan else {
            panic!("expected Update, got {plan:?}");
        };
        // A semi join outputs the left side only, so the source keeps the
        // table's shape (and row identity) with no restoring projection.
        assert!(
            matches!(
                *source,
                PhysicalPlan::HashJoin {
                    join_type: JoinType::Semi,
                    ..
                }
            ),
            "expected the semi join directly, got {source:?}"
        );
    }

    #[test]
    fn update_source_shape_is_restored_after_apply_hoisting() {
        let catalog = catalog_with_users_and_accounts();
        // Non-equality correlation: the Apply widens the source, so the
        // restoring projection narrows it back to the table's two columns.
        let plan = plan_sql(
            "update users set name = 'x' where exists \
             (select 1 from accounts where accounts.owner > users.name)",
            &catalog,
        );
        let PhysicalPlan::Update { source, .. } = plan else {
            panic!("expected Update, got {plan:?}");
        };
        let PhysicalPlan::Projection {
            expressions,
            output_schema,
            ..
        } = *source
        else {
            panic!("expected shape-restoring Projection, got {source:?}");
        };
        assert_eq!(expressions.len(), 2);
        assert_eq!(output_schema.len(), 2);
    }

    #[test]
    fn uncorrelated_subquery_is_not_hoisted() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select id from users where exists (select 1 from accounts)",
            &catalog,
        );
        assert!(
            !format!("{plan:?}").contains("Apply"),
            "uncorrelated subqueries stay with the pre-pass, got {plan:?}"
        );
    }

    // --- LATERAL (docs/specs/subqueries.md §7, milestone S4) ---

    #[test]
    fn comma_lateral_lowers_to_lateral_apply() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select u.id, l.m from users u, \
             lateral (select max(id) as m from accounts a where a.owner = u.name) l",
            &catalog,
        );
        let PhysicalPlan::Projection { source, .. } = plan else {
            panic!("expected Projection, got {plan:?}");
        };
        let PhysicalPlan::Apply {
            input,
            correlations,
            kind:
                ApplyKind::Lateral {
                    left_join: false,
                    condition: None,
                    output_schema,
                },
            ..
        } = *source
        else {
            panic!("expected Apply(Lateral), got {source:?}");
        };
        assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
        assert_eq!(correlations.len(), 1);
        assert_eq!(output_schema.len(), 1);
        assert_eq!(output_schema[0].name, "m");
    }

    #[test]
    fn left_join_lateral_null_pads() {
        let catalog = catalog_with_users_and_accounts();
        let plan = plan_sql(
            "select u.id from users u left join \
             lateral (select id from accounts a where a.owner = u.name) l on true",
            &catalog,
        );
        assert!(
            format!("{plan:?}").contains("left_join: true"),
            "expected a left lateral Apply, got {plan:?}"
        );
    }

    #[test]
    fn sibling_lateral_in_non_first_join_rebases_onto_subtree() {
        let catalog = catalog_with_users_and_accounts();
        // The lateral's join is not the first FROM item; its sibling
        // reference (a.id, global slot 2) is rebased onto the join subtree
        // (local slot 0) when the Apply is lowered.
        let plan = plan_sql(
            "select l.x from users u2, accounts a join \
             lateral (select a.id as x) l on true",
            &catalog,
        );
        fn find_lateral_correlations(plan: &PhysicalPlan) -> Option<&Vec<BoundExpr>> {
            match plan {
                PhysicalPlan::Apply {
                    correlations,
                    kind: ApplyKind::Lateral { .. },
                    ..
                } => Some(correlations),
                PhysicalPlan::Projection { source, .. }
                | PhysicalPlan::Filter { source, .. }
                | PhysicalPlan::Sort { source, .. }
                | PhysicalPlan::Limit { source, .. } => find_lateral_correlations(source),
                PhysicalPlan::NestedLoopJoin { left, right, .. }
                | PhysicalPlan::HashJoin { left, right, .. } => {
                    find_lateral_correlations(left).or_else(|| find_lateral_correlations(right))
                }
                _ => None,
            }
        }
        let correlations =
            find_lateral_correlations(&plan).expect("plan should contain the lateral Apply");
        assert!(
            matches!(correlations[0], BoundExpr::InputRef { slot: 0, .. }),
            "sibling reference should be rebased to the subtree row, got {:?}",
            correlations[0]
        );

        // A reference CROSSING the join boundary (to the comma sibling u2)
        // cannot be supplied by the Apply's input and stays rejected.
        let err = bind(
            &parse(
                "select 1 from users u2, accounts a join \
                 lateral (select u2.id as v) l on true",
            )
            .unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(err.message.contains("outside that join"), "{}", err.message);
    }

    #[test]
    fn chained_only_lateral_lowers_to_unit_input_apply() {
        let catalog = catalog_with_users_and_accounts();
        // Two chained-only laterals at the same level: each must carry its
        // OWN correlation list on an Apply node (embedding the body OuterRefs
        // against the enclosing accumulator would misalign l2's slot).
        let plan = plan_sql(
            "select id from users u where exists \
             (select 1 from lateral (select u.id as p) l1, \
              lateral (select u.name as y) l2 where l2.y = u.name)",
            &catalog,
        );
        let plan_text = format!("{plan:?}");
        assert_eq!(
            plan_text.matches("kind: Lateral").count(),
            2,
            "expected two lateral Apply nodes, got {plan:?}"
        );
    }

    #[test]
    fn lateral_restrictions_are_rejected() {
        let catalog = catalog_with_users_and_accounts();
        // RIGHT/FULL JOIN LATERAL.
        let err = bind(
            &parse(
                "select u.id from users u right join \
                 lateral (select id from accounts a where a.owner = u.name) l on true",
            )
            .unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        // A sibling-referencing LATERAL as the LEFT item of an explicit join.
        let err = bind(
            &parse(
                "select 1 from users u, \
                 lateral (select u.id as x) l join accounts a on a.id = l.x",
            )
            .unwrap(),
            &catalog,
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(err.message.contains("right side"), "{}", err.message);
    }
}
