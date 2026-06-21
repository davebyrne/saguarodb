mod binder;
mod bound;
mod explain;
mod expr;
mod logical;
mod params;
mod physical;

pub use binder::{bind, bind_parameterized};
pub use bound::{BoundFrom, BoundInsertSource, BoundSelect, BoundSelectItem, BoundStatement};
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
    use common::{DataType, ErrorKind, PRIMARY_KEY_INDEX_ID, ParsedColumnDef, SqlState, Value};
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
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
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
                    },
                    ParsedColumnDef {
                        name: "owner".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
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
                    },
                    ParsedColumnDef {
                        name: "label".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
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
        let stmt =
            parse("select a.id from users as a join users as b on a.id = b.id").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };
        let BoundFrom::Join { left, right, .. } = select.from else {
            panic!("expected join");
        };
        let BoundFrom::Table { binding: left_binding, .. } = *left else {
            panic!("expected left table");
        };
        let BoundFrom::Table { binding: right_binding, .. } = *right else {
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
    fn binder_desugars_comma_from_list_into_cross_join() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select users.id from users, accounts").unwrap();
        let bound = bind(&stmt, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected bound select");
        };
        assert!(matches!(
            select.from,
            BoundFrom::Join { join_type: JoinType::Cross, condition: None, .. }
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
}
