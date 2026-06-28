mod binder;
mod bound;
mod explain;
mod expr;
mod logical;
mod params;
mod physical;
mod simplify;

pub use binder::{bind, bind_parameterized};
pub use bound::{
    BoundDistinct, BoundFrom, BoundInsertSource, BoundSelect, BoundSelectItem, BoundStatement,
};
pub use explain::format_explain;
pub use expr::{
    AggregateExpr, AggregateFunc, BinOp, BoundExpr, BoundOrderByItem, JoinType, UnaryOp,
};
pub use logical::{LogicalPlan, logical_plan};
pub use params::{collect_param_types, substitute_params};
pub use physical::{PhysicalPlan, physical_plan};

#[cfg(test)]
mod tests {
    use catalog::{CatalogManager, MemoryCatalog};
    use common::{
        CopyDirection, CopyFormat, CopyOptions, DataType, ErrorKind, PRIMARY_KEY_INDEX_ID,
        ParsedColumnDef, SqlState, Value,
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
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap();
        catalog
    }

    /// `users` plus a non-unique secondary index `users_name` on `name`
    /// (index id 1, since the primary-key index is the reserved id 0).
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
                    },
                    ParsedColumnDef {
                        name: "owner".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap();
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
                    },
                    ParsedColumnDef {
                        name: "label".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                    },
                ],
                vec!["code".to_string()],
            )
            .unwrap();
        catalog
    }

    #[test]
    fn binder_resolves_unqualified_column_to_input_ref_slot() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where name = 'Ada'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };

        assert_eq!(select.output_schema[0].name, "id");
        assert!(matches!(
            select.filter,
            Some(BoundExpr::BinaryOp { ref left, .. })
                if matches!(left.as_ref(), BoundExpr::InputRef { column: 1, slot: 1, .. })
        ));
    }

    #[test]
    fn binder_resolves_order_by_ordinal_to_output_column() {
        let catalog = catalog_with_users();
        let stmt = parse("select name, id from users order by 2").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };

        assert_eq!(select.order_by.len(), 1);
        // Output column 2 is `id`, which resolves to InputRef column 0, slot 0 —
        // not the constant literal 2.
        assert!(matches!(
            select.order_by[0].expr,
            BoundExpr::InputRef {
                column: 0,
                slot: 0,
                ..
            }
        ));
        assert!(select.order_by[0].ascending);
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

        let BoundStatement::Select(select) = bound else {
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
    }

    #[test]
    fn binder_types_null_in_list_from_list_values() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where null in (1, 2)").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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
    fn binder_rejects_composite_primary_key_for_v1() {
        let catalog = catalog_with_users();
        let stmt =
            parse("create table teams (id integer, org integer, primary key (id, org))").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
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
        assert!(format!("{physical:?}").contains(&format!("index: {PRIMARY_KEY_INDEX_ID}")));
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
            // The secondary index is id 1 (the primary-key index is id 0).
            assert!(
                text.contains("index: 1"),
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
        assert!(text.contains(&format!("index: {PRIMARY_KEY_INDEX_ID}")));
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
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

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };
        let BoundFrom::Join { left, right, .. } = select.from else {
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

        let BoundStatement::Select(select) = bound else {
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
    fn binder_rejects_drop_of_unknown_index() {
        let catalog = catalog_with_users();
        let stmt = parse("drop index missing").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn binder_rejects_table_without_primary_key() {
        let catalog = MemoryCatalog::empty();
        let stmt = parse("create table t (a integer not null)").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_binds_between_predicate() {
        let catalog = catalog_with_users();
        let stmt = parse("select name from users where id between 1 and 10").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };
        assert!(matches!(select.filter, Some(BoundExpr::Between { .. })));
    }

    #[test]
    fn binder_binds_like_predicate() {
        let catalog = catalog_with_users();
        let stmt = parse("select id from users where name like 'A%'").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };
        assert!(matches!(select.filter, Some(BoundExpr::Like { .. })));
    }

    #[test]
    fn binder_desugars_coalesce_to_case_with_tight_nullability() {
        let catalog = catalog_with_users();
        let stmt = parse("select coalesce(name, 'fallback') from users").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();
        let BoundStatement::Select(select) = bound else {
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
        let BoundStatement::Select(select) = bound else {
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
        let BoundStatement::Select(select) = bound else {
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
        let BoundStatement::Select(select) = bound else {
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
            let BoundStatement::Select(select) = bound else {
                panic!("expected bound select for {sql}");
            };
            assert_eq!(select.columns[0].expr.data_type(), expected, "for `{sql}`");
        }

        // MOD is integer-only; a double argument is a type mismatch.
        let err = bind(&parse("select mod(2.5, 1.0) from users").unwrap(), &catalog).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn binder_desugars_comma_from_list_into_cross_join() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select users.id from users, accounts").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };
        assert!(matches!(
            select.from,
            BoundFrom::Join {
                join_type: JoinType::Cross,
                condition: None,
                ..
            }
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
                columns: all_columns,
                direction: CopyDirection::From,
                options: CopyOptions::defaults_for(CopyFormat::Text),
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
}
