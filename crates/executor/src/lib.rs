pub mod copy;
mod expr;
mod query;
mod result;
mod subquery;

pub mod ops;

#[cfg(test)]
pub mod test_support;

pub use common::{ExecRow, RowIdentity};
pub use expr::eval_expr;
pub use query::{CopyIn, CopyOut, ExecutionContext, PlanExecutor, QueryEngine, RowSink};
pub use result::{CopyJob, ExecutionResult};

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use catalog::{CatalogManager, MemoryCatalog};
    use common::{
        ColumnDef, ColumnDefault, ColumnInfo, CompressionSetting, CopyFormat, CopyOptions,
        DataType, ExecRow, Key, POSTGRES_COMPAT_VERSION, ParsedColumnDef, RelationKind, Result,
        Row, RowId, RowIdentity, SequenceManager, SessionInfo, SessionSequenceState, SqlState,
        StatementContext, TableSchema, ToastOptions, Value,
    };
    use planner::{BinOp, BoundExpr, PhysicalPlan, UnaryOp};

    use crate::ops::{join_rows, project_row};
    use crate::test_support::{ExecutorHarness, MemoryStorage};
    use crate::{ExecutionContext, ExecutionResult, QueryEngine, eval_expr};
    use storage::SchemaOperations;

    #[derive(Debug, Default)]
    struct TestSequenceManager {
        next_values: Mutex<Vec<i64>>,
        set_calls: Mutex<Vec<(u64, u32, i64, bool)>>,
    }

    impl TestSequenceManager {
        fn with_next_values(values: Vec<i64>) -> Self {
            Self {
                next_values: Mutex::new(values.into_iter().rev().collect()),
                set_calls: Mutex::new(Vec::new()),
            }
        }

        fn set_calls(&self) -> Vec<(u64, u32, i64, bool)> {
            self.set_calls.lock().unwrap().clone()
        }
    }

    impl SequenceManager for TestSequenceManager {
        fn sequence_exists(&self, _sequence: u32) -> Result<bool> {
            Ok(true)
        }

        fn nextval(&self, _txn_id: u64, _sequence: u32) -> Result<i64> {
            self.next_values
                .lock()
                .unwrap()
                .pop()
                .ok_or_else(|| common::DbError::internal("no next test sequence value"))
        }

        fn setval(&self, txn_id: u64, sequence: u32, value: i64, is_called: bool) -> Result<i64> {
            self.set_calls
                .lock()
                .unwrap()
                .push((txn_id, sequence, value, is_called));
            Ok(value)
        }
    }

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

        assert_eq!(
            eval_expr(&StatementContext::new(0), &expr, &row).unwrap(),
            Value::Boolean(false)
        );
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

        let err = eval_expr(&StatementContext::new(0), &expr, &row).unwrap_err();
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

        let err = eval_expr(&StatementContext::new(0), &expr, &row).unwrap_err();
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

        let err = eval_expr(&StatementContext::new(0), &expr, &row).unwrap_err();
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

        let err = eval_expr(&StatementContext::new(0), &expr, &row).unwrap_err();
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
            &StatementContext::new(0),
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
    fn on_conflict_do_nothing_skips_within_statement_duplicate() {
        // The conflict probe uses snapshot visibility including the statement's own
        // earlier inserts, so a duplicate key within one multi-row INSERT is skipped.
        let harness = ExecutorHarness::with_users();
        let result = harness
            .execute(
                "insert into users (id, name) values (5, 'a'), (5, 'b'), (6, 'c') \
                 on conflict (id) do nothing",
            )
            .unwrap();
        assert_eq!(
            result,
            ExecutionResult::Modified {
                command: "INSERT".to_string(),
                count: 2
            }
        );
        let rows = harness
            .select_rows("select id, name from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(5), Value::Text("a".to_string())]
                },
                Row {
                    values: vec![Value::Integer(6), Value::Text("c".to_string())]
                },
            ]
        );
    }

    #[test]
    fn on_conflict_do_update_upserts_existing_row() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        // The conflicting row is updated to excluded.name; the count is 1 (the upsert).
        let result = harness
            .execute(
                "insert into users (id, name) values (1, 'Lovelace') \
                 on conflict (id) do update set name = excluded.name",
            )
            .unwrap();
        assert_eq!(
            result,
            ExecutionResult::Modified {
                command: "INSERT".to_string(),
                count: 1
            }
        );
        let rows = harness
            .select_rows("select id, name from users order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1), Value::Text("Lovelace".to_string())]
            }]
        );
    }

    #[test]
    fn insert_row_does_not_evaluate_default_for_explicit_column() {
        let schema = TableSchema {
            id: 1,
            name: "t".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: Some(ColumnDefault::Nextval(1)),
                pg_type: None,
            }],
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
        };

        let row = crate::query::build_insert_row(
            &common::StatementContext::new(0),
            &schema,
            &[0],
            vec![Value::Integer(9)],
        )
        .unwrap();

        assert_eq!(
            row,
            Row {
                values: vec![Value::Integer(9)]
            }
        );
    }

    #[test]
    fn insert_row_evaluates_sequence_default_for_omitted_column() {
        let manager = Arc::new(TestSequenceManager::with_next_values(vec![42]));
        let session_sequences = Arc::new(SessionSequenceState::new());
        let statement = StatementContext::new(7)
            .with_sequence_manager(manager)
            .with_session_sequences(session_sequences.clone());
        let schema = TableSchema {
            id: 1,
            name: "t".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: Some(ColumnDefault::Nextval(1)),
                pg_type: None,
            }],
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
        };

        let row = crate::query::build_insert_row(&statement, &schema, &[], vec![]).unwrap();

        assert_eq!(
            row,
            Row {
                values: vec![Value::Integer(42)]
            }
        );
        assert_eq!(session_sequences.currval(1).unwrap(), Some(42));
    }

    #[test]
    fn scalar_sequence_functions_use_context_and_session_state() {
        let manager = Arc::new(TestSequenceManager::with_next_values(vec![5]));
        let session_sequences = Arc::new(SessionSequenceState::new());
        let statement = StatementContext::new(23)
            .with_sequence_manager(manager.clone())
            .with_session_sequences(session_sequences.clone());
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let nextval = BoundExpr::Nextval {
            sequence: 1,
            data_type: DataType::Integer,
            nullable: false,
        };
        let currval = BoundExpr::Currval {
            sequence: 1,
            data_type: DataType::Integer,
            nullable: false,
        };

        let err = eval_expr(&statement, &currval, &row).unwrap_err();
        assert_eq!(err.code, SqlState::ObjectNotInPrerequisiteState);
        assert_eq!(
            eval_expr(&statement, &nextval, &row).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            eval_expr(&statement, &currval, &row).unwrap(),
            Value::Integer(5)
        );

        let setval = BoundExpr::Setval {
            sequence: 1,
            value: Box::new(BoundExpr::Literal {
                value: Value::Integer(9),
                data_type: DataType::Integer,
                nullable: false,
            }),
            is_called: Some(Box::new(BoundExpr::Literal {
                value: Value::Boolean(false),
                data_type: DataType::Boolean,
                nullable: false,
            })),
            data_type: DataType::Integer,
            nullable: false,
        };

        assert_eq!(
            eval_expr(&statement, &setval, &row).unwrap(),
            Value::Integer(9)
        );
        assert_eq!(manager.set_calls(), vec![(23, 1, 9, false)]);
        assert_eq!(session_sequences.currval(1).unwrap(), Some(5));

        let fresh_session = Arc::new(SessionSequenceState::new());
        let fresh_statement = StatementContext::new(24)
            .with_sequence_manager(manager.clone())
            .with_session_sequences(fresh_session.clone());
        assert_eq!(
            eval_expr(&fresh_statement, &setval, &row).unwrap(),
            Value::Integer(9)
        );
        assert_eq!(fresh_session.currval(1).unwrap(), None);

        let setval_called = BoundExpr::Setval {
            sequence: 1,
            value: Box::new(BoundExpr::Literal {
                value: Value::Integer(11),
                data_type: DataType::Integer,
                nullable: false,
            }),
            is_called: None,
            data_type: DataType::Integer,
            nullable: false,
        };
        assert_eq!(
            eval_expr(&fresh_statement, &setval_called, &row).unwrap(),
            Value::Integer(11)
        );
        assert_eq!(fresh_session.currval(1).unwrap(), Some(11));
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
        let storage = MemoryStorage::empty();
        storage
            .create_table(&StatementContext::new(0), &schema)
            .unwrap();
        let cancel = std::sync::atomic::AtomicBool::new(false);
        let ctx = ExecutionContext {
            statement: StatementContext::new(1),
            catalog: &catalog,
            storage: &storage,
            schema_ops: &storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
        };
        let plan = PhysicalPlan::Insert {
            table: schema.id,
            columns: vec![0, 1],
            on_conflict: None,
            returning: None,
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
                        pg_type: None,
                    },
                    ColumnInfo {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        table_id: None,
                        column_id: None,
                        pg_type: None,
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
    fn write_rejects_out_of_range_narrow_integer_columns() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table nums (id integer primary key, small smallint, medium integer)")
            .unwrap();
        // In-range values are accepted.
        harness
            .execute("insert into nums (id, small, medium) values (1, 100, 100000)")
            .unwrap();
        // A smallint value outside i16 is rejected (22003), not truncated.
        let err = harness
            .execute("insert into nums (id, small, medium) values (2, 40000, 1)")
            .unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
        // An INTEGER (int4) value outside i32 is likewise rejected.
        let err = harness
            .execute("insert into nums (id, small, medium) values (3, 1, 5000000000)")
            .unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
        // The same check applies to UPDATE.
        let err = harness
            .execute("update nums set small = 40000")
            .unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
    }

    #[test]
    fn cast_to_narrow_integer_rejects_out_of_range() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'a')")
            .unwrap();
        // An in-range cast succeeds.
        harness
            .execute("select cast(100 as smallint) from users")
            .unwrap();
        // A cast to a narrow width outside its range is rejected (22003).
        let err = harness
            .execute("select cast(40000 as smallint) from users")
            .unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
        let err = harness
            .execute("select cast(5000000000 as integer) from users")
            .unwrap_err();
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
    fn scalar_subquery_in_projection_returns_single_value() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table accounts (id integer primary key, owner text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into accounts (id, owner) values (10, 'a'), (20, 'b')")
            .unwrap();

        let rows = harness
            .select_rows("select name, (select max(id) from accounts) from users")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Text("Ada".to_string()), Value::Integer(20)]
            }]
        );
    }

    #[test]
    fn scalar_subquery_empty_result_is_null() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table accounts (id integer primary key, owner text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let rows = harness
            .select_rows("select (select id from accounts where id = 999) from users")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Null]
            }]
        );
    }

    #[test]
    fn scalar_subquery_with_more_than_one_row_errors() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table accounts (id integer primary key, owner text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into accounts (id, owner) values (10, 'a'), (20, 'b')")
            .unwrap();

        let err = harness
            .execute("select (select id from accounts) from users")
            .unwrap_err();
        assert_eq!(err.code, SqlState::CardinalityViolation);
    }

    #[test]
    fn scalar_subquery_in_where_filters_rows() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table accounts (id integer primary key, owner text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'Ada'), (2, 'Grace')")
            .unwrap();
        harness
            .execute("insert into accounts (id, owner) values (2, 'a')")
            .unwrap();

        let rows = harness
            .select_rows("select name from users where id = (select max(id) from accounts)")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Text("Grace".to_string())]
            }]
        );
    }

    #[test]
    fn in_subquery_keeps_matching_rows() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table vals (id integer primary key, v integer)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();
        harness
            .execute("insert into vals (id, v) values (10, 1), (20, 3)")
            .unwrap();

        let rows = harness
            .select_rows("select id from users where id in (select v from vals) order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(1)]
                },
                Row {
                    values: vec![Value::Integer(3)]
                },
            ]
        );
    }

    #[test]
    fn not_in_subquery_with_null_yields_no_rows() {
        // SQL three-valued logic: `x NOT IN (.. NULL ..)` is never TRUE, so a NULL
        // in the subquery result removes every row.
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table vals (id integer primary key, v integer)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b')")
            .unwrap();
        harness
            .execute("insert into vals (id, v) values (10, 1), (20, null)")
            .unwrap();

        let rows = harness
            .select_rows("select id from users where id not in (select v from vals)")
            .unwrap();
        assert!(rows.is_empty(), "got {rows:?}");
    }

    #[test]
    fn not_in_subquery_without_null_keeps_non_members() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table vals (id integer primary key, v integer)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();
        harness
            .execute("insert into vals (id, v) values (10, 1), (20, 2)")
            .unwrap();

        let rows = harness
            .select_rows("select id from users where id not in (select v from vals) order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(3)]
            }]
        );
    }

    #[test]
    fn exists_subquery_gates_all_rows() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table accounts (id integer primary key, owner text)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b')")
            .unwrap();

        // Empty accounts: EXISTS is false -> no rows; NOT EXISTS is true -> all rows.
        assert!(
            harness
                .select_rows("select id from users where exists (select 1 from accounts)")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            harness
                .select_rows("select id from users where not exists (select 1 from accounts)")
                .unwrap()
                .len(),
            2
        );

        // Non-empty accounts flips both.
        harness
            .execute("insert into accounts (id, owner) values (10, 'x')")
            .unwrap();
        assert_eq!(
            harness
                .select_rows("select id from users where exists (select 1 from accounts)")
                .unwrap()
                .len(),
            2
        );
        assert!(
            harness
                .select_rows("select id from users where not exists (select 1 from accounts)")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn derived_table_projects_columns() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b')")
            .unwrap();

        let rows = harness
            .select_rows("select x from (select id as x from users) d order by x")
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
            ]
        );
    }

    #[test]
    fn derived_table_column_aliases_rename_output() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'a')")
            .unwrap();

        let rows = harness
            .select_rows("select y from (select id from users) d(y)")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1)]
            }]
        );
    }

    #[test]
    fn derived_table_with_outer_filter() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();

        // The outer WHERE references the derived column and is applied above the
        // derived sub-plan.
        let rows = harness
            .select_rows("select x from (select id as x from users) d where x > 1 order by x")
            .unwrap();
        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Integer(2)]
                },
                Row {
                    values: vec![Value::Integer(3)]
                },
            ]
        );
    }

    #[test]
    fn derived_table_joined_with_base_table() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'a'), (2, 'b')")
            .unwrap();

        let rows = harness
            .select_rows(
                "select users.name, d.x from users \
                 join (select id as x from users where id = 2) d on users.id = d.x",
            )
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Text("b".to_string()), Value::Integer(2)]
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
    fn system_information_functions_read_session_info() {
        let ctx = StatementContext::new(0).with_session_info(Arc::new(SessionInfo {
            user: "alice".to_string(),
            database: "appdb".to_string(),
            backend_pid: 4242,
        }));
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let eval = |name: &str, data_type: DataType| {
            eval_expr(
                &ctx,
                &BoundExpr::Function {
                    name: name.to_string(),
                    args: Vec::new(),
                    data_type,
                    nullable: false,
                },
                &row,
            )
            .unwrap()
        };

        assert_eq!(
            eval("version", DataType::Text),
            Value::Text(format!(
                "PostgreSQL {} (SaguaroDB {})",
                POSTGRES_COMPAT_VERSION,
                env!("CARGO_PKG_VERSION")
            ))
        );
        assert_eq!(
            eval("current_database", DataType::Text),
            Value::Text("appdb".to_string())
        );
        assert_eq!(
            eval("current_catalog", DataType::Text),
            Value::Text("appdb".to_string())
        );
        assert_eq!(
            eval("current_schema", DataType::Text),
            Value::Text("public".to_string())
        );
        assert_eq!(
            eval("current_user", DataType::Text),
            Value::Text("alice".to_string())
        );
        assert_eq!(
            eval("session_user", DataType::Text),
            Value::Text("alice".to_string())
        );
        assert_eq!(
            eval("user", DataType::Text),
            Value::Text("alice".to_string())
        );
        assert_eq!(
            eval("pg_backend_pid", DataType::Integer),
            Value::Integer(4242)
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

        let err = eval_expr(&StatementContext::new(0), &expr, &row).unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);
    }

    #[test]
    fn mod_of_min_integer_by_negative_one_is_zero() {
        // `i64::MIN % -1` overflows `checked_rem`, but the remainder is 0 (matching
        // PostgreSQL). `i64::MIN` cannot be written as a literal, so evaluate directly.
        let row = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let int = |value| BoundExpr::Literal {
            value: Value::Integer(value),
            data_type: DataType::Integer,
            nullable: false,
        };
        let expr = BoundExpr::Function {
            name: "mod".to_string(),
            args: vec![int(i64::MIN), int(-1)],
            data_type: DataType::Integer,
            nullable: false,
        };

        assert_eq!(
            eval_expr(&StatementContext::new(0), &expr, &row).unwrap(),
            Value::Integer(0)
        );
    }

    #[test]
    fn coalesce_returns_first_non_null_argument() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, null)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'set')")
            .unwrap();

        let rows = harness
            .select_rows("select coalesce(name, 'fallback') from users order by id")
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Text("fallback".to_string())],
                },
                Row {
                    values: vec![Value::Text("set".to_string())],
                },
            ]
        );
    }

    #[test]
    fn nullif_returns_null_when_arguments_are_equal() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (5, 'a')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (7, 'b')")
            .unwrap();

        let rows = harness
            .select_rows("select nullif(id, 5) from users order by id")
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    values: vec![Value::Null],
                },
                Row {
                    values: vec![Value::Integer(7)],
                },
            ]
        );
    }

    #[test]
    fn is_distinct_from_treats_nulls_safely() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, null)")
            .unwrap();

        let rows = harness
            .select_rows(
                "select name is distinct from null, name is not distinct from null, \
                 id is distinct from id from users",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    // NULL is *not* distinct from NULL.
                    Value::Boolean(false),
                    Value::Boolean(true),
                    // 1 is *not* distinct from 1.
                    Value::Boolean(false),
                ],
            }]
        );
    }

    #[test]
    fn ilike_matches_case_insensitively() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'bob')")
            .unwrap();

        let rows = harness
            .select_rows("select id from users where name ilike 'a%' order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1)],
            }]
        );

        let rows = harness
            .select_rows("select id from users where name not ilike 'a%' order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(2)],
            }]
        );
    }

    #[test]
    fn like_escape_clause_changes_escape_character() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, '100%')")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, '100x')")
            .unwrap();

        // With `ESCAPE '!'`, `!%` is a literal percent sign, so only '100%' matches.
        let rows = harness
            .select_rows("select id from users where name like '100!%' escape '!' order by id")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Integer(1)],
            }]
        );
    }

    #[test]
    fn math_functions_over_integer_and_double() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table m (id integer primary key, d double precision)")
            .unwrap();
        harness
            .execute("insert into m (id, d) values (1, 2.5)")
            .unwrap();

        let rows = harness
            .select_rows(
                "select floor(d), ceil(d), round(d), round(3.5), sqrt(9), \
                 power(2.0, 3.0), power(2, 10), floor(id), abs(-7), abs(-2.5), \
                 mod(7, 3) from m",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Float(2.0_f64.into()),    // floor(2.5)
                    Value::Float(3.0_f64.into()),    // ceil(2.5)
                    Value::Float(2.0_f64.into()),    // round(2.5) ties to even
                    Value::Float(4.0_f64.into()),    // round(3.5) ties to even
                    Value::Float(3.0_f64.into()),    // sqrt(9), integer widened
                    Value::Float(8.0_f64.into()),    // power(2.0, 3.0)
                    Value::Float(1024.0_f64.into()), // power(2, 10)
                    Value::Integer(1),               // floor(id) keeps integer
                    Value::Integer(7),               // abs(-7)
                    Value::Float(2.5_f64.into()),    // abs(-2.5)
                    Value::Integer(1),               // mod(7, 3)
                ],
            }]
        );
    }

    #[test]
    fn math_function_type_and_domain_errors() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'x')")
            .unwrap();

        // FLOOR of text is a binder type mismatch.
        let err = harness
            .execute("select floor(name) from users")
            .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);

        // SQRT of a negative number is a runtime out-of-range error.
        let err = harness.execute("select sqrt(-4.0) from users").unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);

        // MOD by zero is division by zero.
        let err = harness.execute("select mod(7, 0) from users").unwrap_err();
        assert_eq!(err.code, SqlState::DivisionByZero);
    }

    #[test]
    fn string_functions_replace_position_left_right() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'hello world')")
            .unwrap();

        let rows = harness
            .select_rows(
                "select replace(name, 'o', '0'), position('world' in name), \
                 left(name, 5), right(name, 5), left(name, -6), right(name, -6) from users",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Text("hell0 w0rld".to_string()),
                    Value::Integer(7), // 'world' begins at the 7th character
                    Value::Text("hello".to_string()),
                    Value::Text("world".to_string()),
                    Value::Text("hello".to_string()), // all but the last 6
                    Value::Text("world".to_string()), // all but the first 6
                ],
            }]
        );
    }

    #[test]
    fn concat_skips_nulls_and_never_returns_null() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, null)")
            .unwrap();
        harness
            .execute("insert into users (id, name) values (2, 'Ada')")
            .unwrap();

        let rows = harness
            .select_rows("select concat(name, '-', name), concat(name) from users order by id")
            .unwrap();

        assert_eq!(
            rows,
            vec![
                Row {
                    // Both NULLs are skipped, leaving just the separator and an
                    // empty string (never NULL).
                    values: vec![Value::Text("-".to_string()), Value::Text(String::new())],
                },
                Row {
                    values: vec![
                        Value::Text("Ada-Ada".to_string()),
                        Value::Text("Ada".to_string()),
                    ],
                },
            ]
        );
    }

    #[test]
    fn statistical_aggregates_compute_variance_stddev_and_bools() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table s (id integer primary key, v integer, flag boolean)")
            .unwrap();
        for (id, v, flag) in [
            (1, 2, "true"),
            (2, 4, "true"),
            (3, 4, "false"),
            (4, 4, "true"),
            (5, 5, "true"),
            (6, 5, "true"),
            (7, 7, "true"),
            (8, 9, "true"),
        ] {
            harness
                .execute(&format!(
                    "insert into s (id, v, flag) values ({id}, {v}, {flag})"
                ))
                .unwrap();
        }

        // mean = 5, sum of squared deviations = 32, n = 8.
        let rows = harness
            .select_rows(
                "select var_pop(v), stddev_pop(v), var_samp(v), stddev_samp(v), \
                 bool_and(flag), bool_or(flag) from s",
            )
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Float(4.0_f64.into()),                 // 32 / 8
                    Value::Float(2.0_f64.into()),                 // sqrt(4)
                    Value::Float((32.0_f64 / 7.0).into()),        // 32 / (8 - 1)
                    Value::Float((32.0_f64 / 7.0).sqrt().into()), // sqrt(32/7)
                    Value::Boolean(false),                        // one flag is false
                    Value::Boolean(true),                         // some flag is true
                ],
            }]
        );
    }

    #[test]
    fn statistical_aggregates_handle_sparse_input() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table s (id integer primary key, v integer, flag boolean)")
            .unwrap();
        harness
            .execute("insert into s (id, v, flag) values (1, 5, null)")
            .unwrap();

        // A single value: population variance is 0, sample variance/stddev are
        // NULL, and BOOL_AND over only-NULL input is NULL.
        let rows = harness
            .select_rows("select var_pop(v), var_samp(v), stddev_samp(v), bool_and(flag) from s")
            .unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Float(0.0_f64.into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
            }]
        );
    }

    #[test]
    fn sum_and_avg_aggregate_over_double() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("create table d (id integer primary key, v double precision)")
            .unwrap();
        harness
            .execute("insert into d (id, v) values (1, 1.5)")
            .unwrap();
        harness
            .execute("insert into d (id, v) values (2, 2.5)")
            .unwrap();

        let rows = harness.select_rows("select sum(v), avg(v) from d").unwrap();
        assert_eq!(
            rows,
            vec![Row {
                values: vec![Value::Float(4.0_f64.into()), Value::Float(2.0_f64.into())],
            }]
        );
    }

    #[test]
    fn extract_pulls_date_and_time_fields() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'x')")
            .unwrap();

        let rows = harness
            .select_rows(
                "select extract(year from date '2024-03-15'), \
                 extract(month from date '2024-03-15'), \
                 extract(day from date '2024-03-15'), \
                 extract(hour from timestamp '2024-03-15 13:24:35.5'), \
                 extract(minute from timestamp '2024-03-15 13:24:35.5'), \
                 extract(second from timestamp '2024-03-15 13:24:35.5') from users",
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![Row {
                values: vec![
                    Value::Float(2024.0_f64.into()),
                    Value::Float(3.0_f64.into()),
                    Value::Float(15.0_f64.into()),
                    Value::Float(13.0_f64.into()),
                    Value::Float(24.0_f64.into()),
                    Value::Float(35.5_f64.into()), // 35 seconds + 0.5 fractional
                ],
            }]
        );
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

    fn text_opts() -> CopyOptions {
        CopyOptions::defaults_for(CopyFormat::Text)
    }

    fn csv_opts() -> CopyOptions {
        CopyOptions::defaults_for(CopyFormat::Csv)
    }

    #[test]
    fn copy_in_inserts_rows_through_the_insert_path() {
        let harness = ExecutorHarness::with_users();
        let count = harness
            .copy_in("users", &[], text_opts(), &[b"3\tcarol\n4\t\\N\n"])
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            harness
                .select_rows("select id, name from users order by id")
                .unwrap(),
            vec![
                Row {
                    values: vec![Value::Integer(3), Value::Text("carol".to_string())]
                },
                Row {
                    values: vec![Value::Integer(4), Value::Null]
                },
            ]
        );
    }

    #[test]
    fn copy_in_csv_skips_header_and_streams_across_chunks() {
        let harness = ExecutorHarness::with_users();
        let mut options = csv_opts();
        options.header = true;
        // The single data row is split across two chunks; the header is dropped.
        let count = harness
            .copy_in("users", &[], options, &[b"id,name\n5,da", b"ve\n"])
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            harness
                .select_rows("select name from users where id = 5")
                .unwrap(),
            vec![Row {
                values: vec![Value::Text("dave".to_string())]
            }]
        );
    }

    #[test]
    fn copy_in_aborts_and_rolls_back_on_bad_value() {
        let harness = ExecutorHarness::with_users();
        let err = harness
            .copy_in("users", &[], text_opts(), &[b"1\tann\nnope\tbob\n"])
            .unwrap_err();
        assert_eq!(err.code, SqlState::InvalidTextRepresentation);
        // The whole COPY aborted: the first row was rolled back too.
        assert!(
            harness
                .select_rows("select id from users")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn copy_out_projects_columns_and_round_trips() {
        let harness = ExecutorHarness::with_users();
        harness
            .execute("insert into users (id, name) values (1, 'ann'), (2, 'bob')")
            .unwrap();

        // Whole table, CSV with header, in primary-key order.
        assert_eq!(
            harness.copy_out("users", &[], csv_opts()).unwrap(),
            b"1,ann\n2,bob\n"
        );
        let mut with_header = csv_opts();
        with_header.header = true;
        assert_eq!(
            harness.copy_out("users", &[], with_header).unwrap(),
            b"id,name\n1,ann\n2,bob\n"
        );
        // Column subset / reorder.
        assert_eq!(
            harness
                .copy_out("users", &["name", "id"], text_opts())
                .unwrap(),
            b"ann\t1\nbob\t2\n"
        );
    }

    /// A `RowSink` that records the schema, every batch's size, and all rows.
    #[derive(Default)]
    struct CollectingSink {
        started: bool,
        columns: Vec<ColumnInfo>,
        batch_sizes: Vec<usize>,
        rows: Vec<Row>,
    }

    impl crate::RowSink for CollectingSink {
        fn start(&mut self, columns: &[ColumnInfo]) -> Result<()> {
            self.started = true;
            self.columns = columns.to_vec();
            Ok(())
        }

        fn push(&mut self, rows: Vec<Row>) -> Result<std::ops::ControlFlow<()>> {
            self.batch_sizes.push(rows.len());
            self.rows.extend(rows);
            Ok(std::ops::ControlFlow::Continue(()))
        }
    }

    /// A `RowSink` that stops the scan once it has collected `limit` rows.
    struct BreakAfterSink {
        limit: usize,
        rows: Vec<Row>,
    }

    impl crate::RowSink for BreakAfterSink {
        fn start(&mut self, _columns: &[ColumnInfo]) -> Result<()> {
            Ok(())
        }

        fn push(&mut self, rows: Vec<Row>) -> Result<std::ops::ControlFlow<()>> {
            self.rows.extend(rows);
            if self.rows.len() >= self.limit {
                Ok(std::ops::ControlFlow::Break(()))
            } else {
                Ok(std::ops::ControlFlow::Continue(()))
            }
        }
    }

    fn seed_five_users(harness: &ExecutorHarness) {
        harness
            .execute(
                "insert into users (id, name) \
                 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            )
            .unwrap();
    }

    #[test]
    fn streamed_select_matches_materialized_across_batch_sizes() {
        let harness = ExecutorHarness::with_users();
        seed_five_users(&harness);
        let sql = "select id, name from users order by id";
        let (columns, rows) = match harness.execute(sql).unwrap() {
            ExecutionResult::Query { columns, rows } => (columns, rows),
            other => panic!("expected a query result, got {other:?}"),
        };

        // Every batch size must reproduce the materialized result exactly: same
        // columns, same rows, same order. Sizes span below, at, and above the row
        // count so the final partial batch and the batch boundary are both covered.
        for batch_size in [1usize, 2, 3, 5, 100] {
            let mut sink = CollectingSink::default();
            let count = harness
                .stream_read_plan(sql, &mut sink, batch_size)
                .unwrap();
            assert!(sink.started, "start not called (batch_size={batch_size})");
            assert_eq!(count, 5, "row count (batch_size={batch_size})");
            assert_eq!(sink.columns, columns, "columns (batch_size={batch_size})");
            assert_eq!(sink.rows, rows, "rows (batch_size={batch_size})");
        }
    }

    #[test]
    fn streamed_select_delivers_expected_batch_boundaries() {
        let harness = ExecutorHarness::with_users();
        seed_five_users(&harness);
        let mut sink = CollectingSink::default();
        harness
            .stream_read_plan("select id from users order by id", &mut sink, 2)
            .unwrap();
        // Five rows in batches of two: [2, 2, 1].
        assert_eq!(sink.batch_sizes, vec![2, 2, 1]);
    }

    #[test]
    fn streamed_select_break_stops_scan_early() {
        let harness = ExecutorHarness::with_users();
        seed_five_users(&harness);
        // One row per batch; break once two rows have arrived. The engine must
        // return immediately, having produced only the two rows it streamed — it
        // must not drain the remaining three.
        let mut sink = BreakAfterSink {
            limit: 2,
            rows: Vec::new(),
        };
        let count = harness
            .stream_read_plan("select id from users order by id", &mut sink, 1)
            .unwrap();
        assert_eq!(count, 2, "streamed count reflects the early stop");
        assert_eq!(sink.rows.len(), 2, "only the pre-break rows were delivered");
    }

    #[test]
    fn streamed_empty_select_still_starts() {
        let harness = ExecutorHarness::with_users();
        let mut sink = CollectingSink::default();
        let count = harness
            .stream_read_plan("select id, name from users", &mut sink, 4)
            .unwrap();
        assert!(sink.started, "start must fire even with no rows");
        assert_eq!(count, 0);
        assert!(sink.rows.is_empty());
        assert!(
            sink.batch_sizes.is_empty(),
            "no batches for an empty result"
        );
        assert_eq!(sink.columns.len(), 2, "schema is still reported");
    }
}
