mod binder;
mod bound;
mod explain;
mod expr;
mod logical;
mod physical;

pub use binder::bind;
pub use bound::{BoundFrom, BoundInsertSource, BoundSelect, BoundSelectItem, BoundStatement};
pub use explain::format_explain;
pub use expr::{
    AggregateExpr, AggregateFunc, BinOp, BoundExpr, BoundOrderByItem, JoinType, UnaryOp,
};
pub use logical::{LogicalPlan, logical_plan};
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
    fn binder_rejects_ambiguous_unqualified_column() {
        let catalog = catalog_with_users_and_accounts();
        let stmt = parse("select id from users join accounts on users.id = accounts.id").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn binder_rejects_insert_select_for_v1() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users select id, name from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_explicit_column_insert_select_before_nullability_checks() {
        let catalog = catalog_with_users();
        let stmt = parse("insert into users (name) select name from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn binder_rejects_unknown_functions_for_v1() {
        let catalog = catalog_with_users();
        let stmt = parse("select lower(name) from users").unwrap();
        let err = bind(&stmt, &catalog).unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
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
}
