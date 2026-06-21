mod expr;
mod query;
mod result;

pub mod ops;

#[cfg(test)]
pub mod test_support;

pub use common::{ExecRow, RowIdentity};
pub use expr::eval_expr;
pub use query::{ExecutionContext, PlanExecutor, QueryEngine};
pub use result::ExecutionResult;

#[cfg(test)]
mod tests {
    use catalog::{CatalogManager, MemoryCatalog};
    use common::{
        ColumnInfo, DataType, ExecRow, Key, ParsedColumnDef, Row, RowId, RowIdentity, SqlState,
        StatementContext, Value,
    };
    use planner::{BinOp, BoundExpr, PhysicalPlan, UnaryOp};

    use crate::ops::{join_rows, project_row};
    use crate::test_support::{ExecutorHarness, MemoryStorage};
    use crate::{ExecutionContext, ExecutionResult, QueryEngine, eval_expr};
    use storage::SchemaOperations;

    #[test]
    fn boolean_and_uses_sql_null_semantics() {
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal {
                value: Value::Null,
                data_type: DataType::Boolean,
                nullable: true,
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::Literal {
                value: Value::Boolean(false),
                data_type: DataType::Boolean,
                nullable: false,
            }),
            data_type: DataType::Boolean,
            nullable: true,
        };

        assert_eq!(eval_expr(&expr, &row).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn boolean_and_rejects_non_boolean_operand() {
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal {
                value: Value::Boolean(false),
                data_type: DataType::Boolean,
                nullable: false,
            }),
            op: BinOp::And,
            right: Box::new(BoundExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            }),
            data_type: DataType::Boolean,
            nullable: false,
        };

        let err = eval_expr(&expr, &row).unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn division_by_zero_returns_sqlstate() {
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal {
                value: Value::Integer(4),
                data_type: DataType::Integer,
                nullable: false,
            }),
            op: BinOp::Div,
            right: Box::new(BoundExpr::Literal {
                value: Value::Integer(0),
                data_type: DataType::Integer,
                nullable: false,
            }),
            data_type: DataType::Integer,
            nullable: false,
        };

        let err = eval_expr(&expr, &row).unwrap_err();
        assert_eq!(err.code, SqlState::DivisionByZero);
    }

    #[test]
    fn integer_overflow_returns_sqlstate() {
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let expr = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal {
                value: Value::Integer(i64::MAX),
                data_type: DataType::Integer,
                nullable: false,
            }),
            op: BinOp::Add,
            right: Box::new(BoundExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            }),
            data_type: DataType::Integer,
            nullable: false,
        };

        let err = eval_expr(&expr, &row).unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
    }

    #[test]
    fn unary_integer_overflow_returns_sqlstate() {
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let expr = BoundExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr: Box::new(BoundExpr::Literal {
                value: Value::Integer(i64::MIN),
                data_type: DataType::Integer,
                nullable: false,
            }),
            data_type: DataType::Integer,
            nullable: false,
        };

        let err = eval_expr(&expr, &row).unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
    }

    #[test]
    fn projection_preserves_row_identity() {
        let input = ExecRow {
            row: Row {
                values: vec![Value::Integer(7), Value::Text("Ada".to_string())],
            },
            identity: Some(RowIdentity {
                row_id: RowId {
                    page_num: 1,
                    slot_num: 0,
                },
                key: Key(vec![Value::Integer(7)]),
            }),
        };

        let projected = project_row(
            input.clone(),
            &[BoundExpr::InputRef {
                input: 1,
                column: 1,
                slot: 1,
                data_type: DataType::Text,
                nullable: false,
            }],
        )
        .unwrap();

        assert_eq!(projected.row.values, vec![Value::Text("Ada".to_string())]);
        assert_eq!(projected.identity, input.identity);
    }

    #[test]
    fn join_clears_row_identity() {
        let joined = join_rows(
            ExecRow {
                row: Row {
                    values: vec![Value::Integer(1)],
                },
                identity: Some(RowIdentity {
                    row_id: RowId {
                        page_num: 1,
                        slot_num: 0,
                    },
                    key: Key(vec![Value::Integer(1)]),
                }),
            },
            ExecRow {
                row: Row {
                    values: vec![Value::Text("x".to_string())],
                },
                identity: Some(RowIdentity {
                    row_id: RowId {
                        page_num: 2,
                        slot_num: 0,
                    },
                    key: Key(vec![Value::Text("x".to_string())]),
                }),
            },
        );

        assert_eq!(joined.identity, None);
    }

    #[test]
    fn update_where_uses_identity_and_changes_only_matching_rows() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        let result = harness
            .execute("update users set name = 'Lovelace' where id = 1")
            .unwrap();
        assert_eq!(
            result,
            ExecutionResult::Modified {
                command: "UPDATE".to_string(),
                count: 1
            }
        );

        let rows = harness
            .select_rows("select id, name from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Text("Lovelace".to_string())]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Text("Grace".to_string())]
                },
            ]
        );
    }

    #[test]
    fn delete_where_deletes_only_matching_rows() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        let result = harness.execute("delete from users where id = 2").unwrap();
        assert_eq!(
            result,
            ExecutionResult::Modified {
                command: "DELETE".to_string(),
                count: 1
            }
        );

        let rows = harness
            .select_rows("select id, name from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1), Value::Text("Ada".to_string())]
            }]
        );
    }

    #[test]
    fn update_where_non_key_filter_preserves_identity() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        let result = harness
            .execute("update users set name = 'Hopper' where name = 'Grace'")
            .unwrap();
        assert_eq!(
            result,
            ExecutionResult::Modified {
                command: "UPDATE".to_string(),
                count: 1
            }
        );

        let rows = harness
            .select_rows("select id, name from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Text("Ada".to_string())]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Text("Hopper".to_string())]
                },
            ]
        );
    }

    #[test]
    fn failed_write_rolls_back_prior_rows_in_statement() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let err = harness
            .execute("insert into users (id, name) values (2, 'Grace'), (1, 'Duplicate')")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);

        let rows = harness
            .select_rows("select id, name from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1), Value::Text("Ada".to_string())]
            }]
        );
    }

    #[test]
    fn insert_rejects_runtime_value_type_mismatch() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
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
        let storage = MemoryStorage::empty();
        storage
            .create_table(&StatementContext { txn_id: 0 }, &schema)
            .unwrap();
        let cancel = std::sync::atomic::AtomicBool::new(false);
        let ctx = ExecutionContext {
            statement: StatementContext { txn_id: 1 },
            catalog: &catalog,
            storage: &storage,
            schema_ops: &storage,
            cancel: &cancel,
        };
        let plan = PhysicalPlan::Insert {
            table: schema.id,
            columns: vec![0, 1],
            source: Box::new(PhysicalPlan::Values {
                rows: vec![vec![
                    BoundExpr::Literal {
                        value: Value::Text("not-an-integer".to_string()),
                        data_type: DataType::Text,
                        nullable: false,
                    },
                    BoundExpr::Literal {
                        value: Value::Text("Ada".to_string()),
                        data_type: DataType::Text,
                        nullable: false,
                    },
                ]],
                output_schema: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: DataType::Text,
                        table_id: None,
                        column_id: None,
                    },
                    ColumnInfo {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        table_id: None,
                        column_id: None,
                    },
                ],
            }),
        };

        let err = QueryEngine.execute(&ctx, &plan).unwrap_err();

        assert_eq!(err.code, SqlState::DatatypeMismatch);
        assert!(err.message.contains("expected column id"));
    }

    #[test]
    fn query_aborts_when_cancellation_requested() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let cancel = std::sync::atomic::AtomicBool::new(true);
        let err = harness
            .execute_with_cancel("select id from users", &cancel)
            .unwrap_err();

        assert_eq!(err.code, SqlState::QueryCanceled);
        assert!(err.message.contains("canceling statement"));
    }

    #[test]
    fn sum_overflow_returns_sqlstate() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (9223372036854775807, 'Max')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'One')")
            .unwrap();

        let err = harness.execute("select sum(id) from users").unwrap_err();

        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
    }

    #[test]
    fn inner_join_returns_only_matching_rows() {
        let harness = ExecutorHarness::with_users();
        seed_users_and_accounts(&harness);

        let rows = harness
            .select_rows(
                "select users.id, accounts.id from users join accounts \
                 on users.name = accounts.owner order by users.id",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1), Value::Integer(10)]
            }]
        );
    }

    #[test]
    fn cross_join_returns_cartesian_product() {
        let harness = ExecutorHarness::with_users();
        seed_users_and_accounts(&harness);

        let rows = harness
            .select_rows(
                "select users.id, accounts.id from users cross join accounts \
                 order by users.id, accounts.id",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Integer(10)]
                },
                Row {
                    values: vec![Value::Integer(1), Value::Integer(20)]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Integer(10)]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Integer(20)]
                },
            ]
        );
    }

    #[test]
    fn left_join_null_extends_unmatched_right_side() {
        let harness = ExecutorHarness::with_users();
        seed_users_and_accounts(&harness);

        let rows = harness
            .select_rows(
                "select users.id, accounts.id from users left join accounts \
                 on users.name = accounts.owner order by users.id",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Integer(10)]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Null]
                },
            ]
        );
    }

    #[test]
    fn right_join_null_extends_unmatched_left_side() {
        let harness = ExecutorHarness::with_users();
        seed_users_and_accounts(&harness);

        let rows = harness
            .select_rows(
                "select users.id, accounts.id from users right join accounts \
                 on users.name = accounts.owner order by accounts.id",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Integer(10)]
                },
                Row {
                    values: vec![Value::Null, Value::Integer(20)]
                },
            ]
        );
    }

    #[test]
    fn full_join_null_extends_both_unmatched_sides() {
        let harness = ExecutorHarness::with_users();
        seed_users_and_accounts(&harness);

        let rows = harness
            .select_rows(
                "select users.id, accounts.id from users full join accounts \
                 on users.name = accounts.owner order by accounts.id",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Integer(10)]
                },
                Row {
                    values: vec![Value::Null, Value::Integer(20)]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Null]
                },
            ]
        );
    }

    #[test]
    fn inner_equi_join_does_not_match_null_keys() {
        let harness = ExecutorHarness::with_users();
        seed_users_and_accounts(&harness);
        harness
            .execute("insert into users (id, name) values (3, null)")
            .unwrap();
        harness
            .execute("insert into accounts (id, owner) values (30, null)")
            .unwrap();

        let rows = harness
            .select_rows(
                "select users.id, accounts.id from users join accounts \
                 on users.name = accounts.owner order by users.id, accounts.id",
            )
            .unwrap();

        // NULL = NULL is never true, so the NULL-keyed rows must not join.
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1), Value::Integer(10)]
            }]
        );
    }

    #[test]
    fn inner_equi_join_matches_on_multiple_keys() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table l (id integer primary key, grp integer, tag text)")
            .unwrap();
        harness
            .execute("create table r (id integer primary key, grp integer, tag text)")
            .unwrap();
        harness
            .execute("insert into l (id, grp, tag) values (1, 100, 'x')")
            .unwrap();
        harness
            .execute("insert into l (id, grp, tag) values (2, 100, 'y')")
            .unwrap();
        harness
            .execute("insert into r (id, grp, tag) values (11, 100, 'x')")
            .unwrap();
        harness
            .execute("insert into r (id, grp, tag) values (12, 200, 'x')")
            .unwrap();

        let rows = harness
            .select_rows(
                "select l.id, r.id from l join r \
                 on l.grp = r.grp and l.tag = r.tag order by l.id, r.id",
            )
            .unwrap();

        // Only l(grp=100, tag='x') matches r(grp=100, tag='x'); the row that
        // agrees on grp but not tag must not join.
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1), Value::Integer(11)]
            }]
        );
    }

    #[test]
    fn aggregate_computes_count_sum_avg_min_max() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        let rows = harness
            .select_rows("select count(*), sum(id), avg(id), min(id), max(id) from users")
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Integer(2),
                    Value::Integer(3),
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Integer(2),
                ]
            }]
        );
    }

    #[test]
    fn insert_select_copies_rows_from_another_table() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table archived_users (id integer primary key, name text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        let result = harness
            .execute("insert into archived_users select id, name from users")
            .unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 2, .. }));

        let rows = harness
            .select_rows("select id, name from archived_users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1), Value::Text("Ada".to_string())]
                },
                Row {
                    values: vec![Value::Integer(2), Value::Text("Grace".to_string())]
                },
            ]
        );
    }

    #[test]
    fn insert_select_applies_where_filter() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table flagged (id integer primary key, name text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        harness
            .execute("insert into flagged select id, name from users where id = 2")
            .unwrap();

        let rows = harness.select_rows("select id from flagged").unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(2)]
            }]
        );
    }

    #[test]
    fn insert_select_from_target_table_sees_only_preexisting_rows() {
        // The Halloween problem: an INSERT ... SELECT that reads its own target
        // must duplicate only the rows that existed before the statement ran.
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();

        let result = harness
            .execute("insert into users select id + 10, name from users")
            .unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 2, .. }));

        let rows = harness
            .select_rows("select id from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1)]
                },
                Row {
                    values: vec![Value::Integer(2)]
                },
                Row {
                    values: vec![Value::Integer(11)]
                },
                Row {
                    values: vec![Value::Integer(12)]
                },
            ]
        );
    }

    #[test]
    fn scalar_string_and_numeric_functions_evaluate() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (-5, '  Ada  ')")
            .unwrap();

        let rows = harness
            .select_rows(
                "select upper(name), lower(name), length(name), trim(name), abs(id) from users",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Text("  ADA  ".to_string()),
                    Value::Text("  ada  ".to_string()),
                    Value::Integer(7),
                    Value::Text("Ada".to_string()),
                    Value::Integer(5),
                ]
            }]
        );
    }

    #[test]
    fn scalar_functions_propagate_null() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, null)")
            .unwrap();

        let rows = harness
            .select_rows("select upper(name), length(name), substring(name, 1, 2) from users")
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Null, Value::Null, Value::Null]
            }]
        );
    }

    #[test]
    fn substring_handles_bounds_and_optional_length() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'hello')")
            .unwrap();

        let rows = harness
            .select_rows(
                "select substring(name, 2, 3), substring(name, 3), substring(name, 0, 3), \
                 substring(name, 10), substring(name, 2, 0) from users",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Text("ell".to_string()),
                    Value::Text("llo".to_string()),
                    Value::Text("he".to_string()),
                    Value::Text(String::new()),
                    Value::Text(String::new()),
                ]
            }]
        );
    }

    #[test]
    fn string_functions_count_characters_not_bytes() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'café')")
            .unwrap();

        let rows = harness
            .select_rows("select length(name), substring(name, 4, 1) from users")
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(4), Value::Text("é".to_string())]
            }]
        );
    }

    #[test]
    fn substring_rejects_negative_length() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'hello')")
            .unwrap();

        let err = harness
            .execute("select substring(name, 2, -1) from users")
            .unwrap_err();

        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn abs_of_min_integer_returns_out_of_range() {
        // `i64::MIN` cannot be written as a SQL literal, so evaluate directly.
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let expr = BoundExpr::Function {
            name: "abs".to_string(),
            args: vec![BoundExpr::Literal {
                value: Value::Integer(i64::MIN),
                data_type: DataType::Integer,
                nullable: false,
            }],
            data_type: DataType::Integer,
            nullable: false,
        };

        let err = eval_expr(&expr, &row).unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
    }

    #[test]
    fn string_concatenation_operator_evaluates_and_propagates_null() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, null)")
            .unwrap();

        let rows = harness
            .select_rows("select name || '!' from users order by id")
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Text("Ada!".to_string())]
                },
                Row {
                    values: vec![Value::Null]
                },
            ]
        );
    }

    fn seed_users_and_accounts(harness: &ExecutorHarness) {
        harness
            .execute("create table accounts (id integer primary key, owner text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Grace')")
            .unwrap();
        harness
            .execute("insert into accounts (id, owner) values (10, 'Ada')")
            .unwrap();
        harness
            .execute("insert into accounts (id, owner) values (20, 'Linus')")
            .unwrap();
    }
}
