mod ast;
mod convert;

pub use ast::{
    Assignment, BinOp, ConflictAction, ConflictTarget, Cte, Distinct, Expr, FetchCount, FromItem,
    FunctionArg, InsertSource, JoinType, OnConflict, OrderByItem, Query, QueryBody, Select,
    SelectItem, SetOp, SetScope, Statement, UnaryOp,
};

use common::Result;

pub fn parse(sql: &str) -> Result<Statement> {
    convert::parse_statement(sql)
}

/// Parse a single SQL scalar expression (not a statement). The binder uses this
/// to re-parse the stored canonical text of a non-constant column `DEFAULT`
/// before binding it.
pub fn parse_expression(sql: &str) -> Result<Expr> {
    convert::parse_expression(sql)
}

#[cfg(test)]
mod tests {
    use common::{
        CompressionSetting, CopyDirection, CopyFormat, CopyOptions, DataType, ErrorKind,
        ParsedColumnDef, ParsedDefault, PgType, QualifiedName, SequenceOptions, SqlState,
        TableOptionPatch, ToastCompression, ToastMode, ToastOptionPatch, Value,
    };

    fn qn(name: &str) -> QualifiedName {
        QualifiedName::unqualified(name)
    }

    use crate::{
        Assignment, BinOp, Expr, FetchCount, FromItem, FunctionArg, InsertSource, JoinType, Query,
        QueryBody, SelectItem, SetOp, SetScope, Statement, UnaryOp, parse,
    };

    fn id_column() -> ParsedColumnDef {
        ParsedColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: true,
            max_length: None,
            default: None,
            pg_type: Some(PgType::Int4),
        }
    }

    #[test]
    fn rejects_multiple_statements() {
        let err = parse("select 1; select 2").unwrap_err();

        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn parses_set_variable_forms() {
        assert_eq!(
            parse("SET extra_float_digits = 3").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "extra_float_digits".to_string(),
                value: "3".to_string(),
            }
        );
        assert_eq!(
            parse("SET datestyle TO 'ISO'").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "datestyle".to_string(),
                value: "ISO".to_string(),
            }
        );
        assert_eq!(
            parse("SET TIME ZONE 'UTC'").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "timezone".to_string(),
                value: "UTC".to_string(),
            }
        );
        assert_eq!(
            parse("SET search_path = \"$user\", public").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "search_path".to_string(),
                value: "$user, public".to_string(),
            }
        );
        assert_eq!(
            parse("SET SESSION statement_timeout = 0").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "statement_timeout".to_string(),
                value: "0".to_string(),
            }
        );
        assert_eq!(
            parse("SET LOCAL statement_timeout = 0").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Local,
                name: "statement_timeout".to_string(),
                value: "0".to_string(),
            }
        );
        assert_eq!(
            parse("SET my_app.batch_size = -2").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "my_app.batch_size".to_string(),
                value: "-2".to_string(),
            }
        );
        assert_eq!(
            parse("SET \"Default_Transaction_Isolation\" TO 'serializable'").unwrap(),
            Statement::SetVariable {
                scope: SetScope::Session,
                name: "default_transaction_isolation".to_string(),
                value: "serializable".to_string(),
            }
        );
    }

    #[test]
    fn set_transaction_forms_are_untouched_by_the_guc_path() {
        assert!(matches!(
            parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap(),
            Statement::SetTransaction { isolation: Some(_) }
        ));
        assert!(matches!(
            parse("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .unwrap(),
            Statement::SetSessionCharacteristics { isolation: Some(_) }
        ));
    }

    #[test]
    fn parses_show_reset_and_discard() {
        assert_eq!(
            parse("SHOW extra_float_digits").unwrap(),
            Statement::ShowVariable {
                name: Some("extra_float_digits".to_string())
            }
        );
        assert_eq!(
            parse("SHOW ALL").unwrap(),
            Statement::ShowVariable { name: None }
        );
        assert_eq!(
            parse("SHOW TIME ZONE").unwrap(),
            Statement::ShowVariable {
                name: Some("timezone".to_string())
            }
        );
        assert_eq!(
            parse("SHOW TRANSACTION ISOLATION LEVEL").unwrap(),
            Statement::ShowVariable {
                name: Some("transaction_isolation".to_string())
            }
        );
        assert_eq!(
            parse("RESET extra_float_digits").unwrap(),
            Statement::ResetVariable {
                name: Some("extra_float_digits".to_string())
            }
        );
        assert_eq!(
            parse("RESET \"Transaction_Isolation\";").unwrap(),
            Statement::ResetVariable {
                name: Some("transaction_isolation".to_string())
            }
        );
        assert_eq!(
            parse("RESET ALL;").unwrap(),
            Statement::ResetVariable { name: None }
        );
        assert_eq!(parse("DISCARD ALL").unwrap(), Statement::DiscardAll);

        let err = parse("DISCARD PLANS").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        for sql in [
            "SET GLOBAL statement_timeout = 0",
            "SET GLOBAL TIME ZONE 'UTC'",
            "SET GLOBAL NAMES utf8",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
    }

    #[test]
    fn parses_sql_cursor_forms() {
        let Statement::DeclareCursor { name, query } =
            parse("DECLARE MyCursor CURSOR FOR SELECT id FROM users").unwrap()
        else {
            panic!("expected DECLARE CURSOR");
        };
        assert_eq!(name, "mycursor");
        let QueryBody::Select(select) = query.body else {
            panic!("expected SELECT cursor body");
        };
        assert!(matches!(
            select.columns.as_slice(),
            [SelectItem::Expression {
                expr: Expr::ColumnRef { table: None, column },
                alias: None,
            }] if column == "id"
        ));
        assert!(matches!(
            select.from.as_slice(),
            [FromItem::Table {
                name,
                alias: None,
            }] if name == &qn("users")
        ));

        for sql in ["FETCH FROM c", "FETCH c", "FETCH FORWARD FROM c"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::FetchCursor {
                    name: "c".to_string(),
                    count: FetchCount::One,
                },
                "for `{sql}`"
            );
        }
        for sql in ["FETCH 2 FROM c", "FETCH FORWARD 2 FROM c"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::FetchCursor {
                    name: "c".to_string(),
                    count: FetchCount::Count(2),
                },
                "for `{sql}`"
            );
        }
        assert_eq!(
            parse("FETCH ALL FROM c").unwrap(),
            Statement::FetchCursor {
                name: "c".to_string(),
                count: FetchCount::All,
            }
        );
        assert_eq!(
            parse("CLOSE C").unwrap(),
            Statement::CloseCursor {
                name: "c".to_string()
            }
        );
    }

    #[test]
    fn rejects_unsupported_sql_cursor_forms() {
        for sql in [
            "DECLARE c SCROLL CURSOR FOR SELECT 1",
            "DECLARE c NO SCROLL CURSOR FOR SELECT 1",
            "DECLARE c BINARY CURSOR FOR SELECT 1",
            "DECLARE c INSENSITIVE CURSOR FOR SELECT 1",
            "DECLARE c ASENSITIVE CURSOR FOR SELECT 1",
            "DECLARE c CURSOR WITH HOLD FOR SELECT 1",
            "DECLARE c CURSOR WITHOUT HOLD FOR SELECT 1",
            "DECLARE c CURSOR FOR VALUES (1)",
            "DECLARE c CURSOR FOR VALUES (1) UNION SELECT 2",
            "FETCH BACKWARD",
            "FETCH BACKWARD FROM c",
            "FETCH ABSOLUTE",
            "FETCH ABSOLUTE 1 FROM c",
            "FETCH RELATIVE",
            "FETCH RELATIVE 1 FROM c",
            "FETCH NEXT",
            "FETCH -1 FROM c",
            "FETCH 1L FROM c",
            "FETCH FORWARD ALL FROM c",
            "CLOSE ALL",
            "DECLARE \"c\" CURSOR FOR SELECT 1",
            "FETCH FROM \"c\"",
            "CLOSE \"c\"",
        ] {
            let err = parse(sql).unwrap_err();
            assert!(
                matches!(
                    err.code,
                    SqlState::SyntaxError | SqlState::FeatureNotSupported
                ),
                "unexpected SQLSTATE {:?} for `{sql}`",
                err.code
            );
        }
    }

    #[test]
    fn numeric_typed_literal_validates_precision_and_scale() {
        // A valid NUMERIC literal parses.
        assert!(parse("select numeric '1.23' from t").is_ok());
        // scale > precision is rejected cleanly (a parse error), not a panic.
        let err = parse("select numeric(2,5) '1.23' from t").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
        // precision beyond the Decimal limit (28) is likewise rejected at parse.
        let err = parse("select numeric(50,2) '1.23' from t").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
    }

    #[test]
    fn parses_select_with_alias_and_qualified_wildcard() {
        let stmt = parse("select users.*, name as n from users where id = 7").unwrap();

        match stmt {
            Statement::Query(Query {
                body: QueryBody::Select(select),
                ..
            }) => {
                assert_eq!(select.columns.len(), 2);
                assert!(matches!(
                    select.columns[0],
                    SelectItem::QualifiedWildcard(ref table) if table == "users"
                ));
                assert!(matches!(
                    select.columns[1],
                    SelectItem::Expression {
                        ref alias,
                        ..
                    } if alias.as_deref() == Some("n")
                ));
                assert!(select.filter.is_some());
            }
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_subquery_in_projection() {
        let stmt = parse("select (select max(id) from accounts) from users").unwrap();

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        let SelectItem::Expression {
            expr: Expr::Subquery(subquery),
            ..
        } = &select.columns[0]
        else {
            panic!("expected scalar subquery, got {:?}", select.columns[0]);
        };
        let QueryBody::Select(inner) = &subquery.body else {
            panic!("expected a plain SELECT subquery body");
        };
        assert!(
            matches!(inner.from.as_slice(), [FromItem::Table { name, .. }] if name == "accounts")
        );
    }

    #[test]
    fn parses_schema_evolution_ddl_surface() {
        assert_eq!(
            parse("alter table users add column if not exists age integer").unwrap(),
            Statement::AlterTableAddColumn {
                table: qn("users"),
                if_not_exists: true,
                column: ParsedColumnDef {
                    name: "age".to_string(),
                    ..id_column()
                },
            }
        );

        assert_eq!(
            parse("alter table users drop column if exists age").unwrap(),
            Statement::AlterTableDropColumn {
                table: qn("users"),
                if_exists: true,
                column: "age".to_string(),
            }
        );

        assert_eq!(
            parse("alter table users add column nickname text not null default 'anon'").unwrap(),
            Statement::AlterTableAddColumn {
                table: qn("users"),
                if_not_exists: false,
                column: ParsedColumnDef {
                    name: "nickname".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    max_length: None,
                    default: Some(ParsedDefault::Const(Value::Text("anon".to_string()))),
                    pg_type: Some(PgType::Text),
                },
            }
        );

        assert_eq!(
            parse("alter table users rename column full_name to name").unwrap(),
            Statement::AlterTableRenameColumn {
                table: qn("users"),
                old_name: "full_name".to_string(),
                new_name: "name".to_string(),
            }
        );

        assert_eq!(
            parse("alter table users rename to accounts").unwrap(),
            Statement::AlterTableRenameTable {
                table: qn("users"),
                new_name: "accounts".to_string(),
            }
        );

        assert_eq!(
            parse("truncate table users").unwrap(),
            Statement::Truncate {
                tables: vec![qn("users")],
            }
        );
    }

    #[test]
    fn parses_view_ddl_surface() {
        let statement =
            parse("create or replace view active_users (id, name) as select id, name from users")
                .unwrap();
        match statement {
            Statement::CreateView {
                name,
                or_replace,
                columns,
                query,
                definition,
            } => {
                assert_eq!(name, "active_users");
                assert!(or_replace);
                assert_eq!(columns, vec!["id", "name"]);
                assert!(matches!(query.body, QueryBody::Select(_)));
                assert!(
                    definition
                        .to_ascii_lowercase()
                        .starts_with("select id, name")
                );
            }
            other => panic!("expected CREATE VIEW, got {other:?}"),
        }

        assert_eq!(
            parse("drop view if exists active_users").unwrap(),
            Statement::DropView {
                name: qn("active_users"),
                if_exists: true,
            }
        );
    }

    #[test]
    fn rejects_unsupported_schema_evolution_forms() {
        assert_eq!(
            parse("alter table users drop column age cascade")
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
        assert_eq!(
            parse("truncate users restart identity").unwrap_err().code,
            SqlState::FeatureNotSupported
        );
        assert_eq!(
            parse("alter table users add column id serial")
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn parses_scalar_subquery_in_where() {
        let stmt =
            parse("select name from users where id = (select min(id) from accounts)").unwrap();

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.filter,
            Some(Expr::BinaryOp {
                op: BinOp::Eq,
                ref right,
                ..
            }) if matches!(**right, Expr::Subquery(_))
        ));
    }

    #[test]
    fn parses_in_subquery_and_not_in_subquery() {
        for (sql, negated) in [
            (
                "select id from users where id in (select id from accounts)",
                false,
            ),
            (
                "select id from users where id not in (select id from accounts)",
                true,
            ),
        ] {
            let Statement::Query(Query {
                body: QueryBody::Select(select),
                ..
            }) = parse(sql).unwrap()
            else {
                panic!("expected select");
            };
            assert!(
                matches!(
                    select.filter,
                    Some(Expr::InSubquery { negated: n, .. }) if n == negated
                ),
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn parses_exists_and_not_exists_subquery() {
        for (sql, negated) in [
            (
                "select id from users where exists (select 1 from accounts)",
                false,
            ),
            (
                "select id from users where not exists (select 1 from accounts)",
                true,
            ),
        ] {
            let Statement::Query(Query {
                body: QueryBody::Select(select),
                ..
            }) = parse(sql).unwrap()
            else {
                panic!("expected select");
            };
            assert!(
                matches!(select.filter, Some(Expr::Exists { negated: n, .. }) if n == negated),
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn parses_derived_table_with_column_aliases() {
        let stmt = parse("select d.x from (select id from users) as d(x)").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.from.as_slice(),
            [FromItem::Derived { alias, column_aliases, .. }]
                if alias == "d" && column_aliases.as_slice() == ["x".to_string()]
        ));
    }

    #[test]
    fn rejects_derived_table_without_alias() {
        let err = parse("select * from (select id from users)").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn parses_top_level_values() {
        let stmt = parse("values (1, 'a'), (2, 'b')").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Values(rows),
            ..
        }) = stmt
        else {
            panic!("expected a VALUES query");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
        assert!(matches!(rows[0][0], Expr::Literal(Value::Integer(1))));
    }

    #[test]
    fn parses_set_operation_with_outer_order_by() {
        // The ORDER BY binds to the whole set operation (the outer Query wrapper),
        // not to the right arm.
        let stmt = parse("select id from a union all select id from b order by 1").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected a query");
        };
        assert_eq!(query.order_by.len(), 1);
        let QueryBody::SetOp { op, all, right, .. } = query.body else {
            panic!("expected a set operation body");
        };
        assert_eq!(op, SetOp::Union);
        assert!(all);
        // The right arm is a plain SELECT with no ORDER BY of its own.
        assert!(matches!(right.body, QueryBody::Select(_)));
        assert!(right.order_by.is_empty());
    }

    #[test]
    fn parses_with_clause() {
        let stmt = parse("with a as (select 1), b(x) as (select 2) select x from b").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected a query");
        };
        assert_eq!(query.with.len(), 2);
        assert_eq!(query.with[0].name, "a");
        assert!(query.with[0].column_aliases.is_empty());
        assert_eq!(query.with[1].name, "b");
        assert_eq!(query.with[1].column_aliases, vec!["x".to_string()]);
        // The CTE body is a full Query.
        assert!(matches!(query.with[0].query.body, QueryBody::Select(_)));
    }

    #[test]
    fn parses_count_star_and_aggregate_distinct_shape() {
        let stmt = parse("select count(*), count(distinct id) from users").unwrap();

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: false },
                ..
            } if name == "count" && matches!(args.as_slice(), [FunctionArg::Wildcard])
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: true },
                ..
            } if name == "count" && matches!(args.as_slice(), [FunctionArg::Expr(_)])
        ));
    }

    #[test]
    fn parses_statement_timestamp_functions() {
        let stmt = parse("select current_timestamp, now()").unwrap();

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: false },
                ..
            } if name == "current_timestamp" && args.is_empty()
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: false },
                ..
            } if name == "now" && args.is_empty()
        ));
    }

    #[test]
    fn normalizes_pg_catalog_qualified_functions() {
        let stmt = parse("select pg_catalog.format_type(23, -1)").unwrap();

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: false },
                ..
            } if name == "format_type" && args.len() == 2
        ));
    }

    #[test]
    fn rejects_non_catalog_qualified_functions() {
        let err = parse("select public.format_type(23, -1)").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(err.message.contains("qualified function names"));
    }

    #[test]
    fn normalizes_trim_and_substring_into_function_calls() {
        let stmt = parse("select trim(name), substring(name, 2, 3) from users").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: false },
                ..
            } if name == "trim" && args.len() == 1
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, distinct: false },
                ..
            } if name == "substring" && args.len() == 3
        ));
    }

    #[test]
    fn normalizes_substring_from_for_syntax_to_function_args() {
        let stmt =
            parse("select substring(name from 2 for 3), substring(name, 2) from users").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, .. },
                ..
            } if name == "substring" && args.len() == 3
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::Function { ref name, ref args, .. },
                ..
            } if name == "substring" && args.len() == 2
        ));
    }

    #[test]
    fn parses_insert_select_even_when_binder_rejects_it() {
        let stmt = parse("insert into users select id, name from old_users").unwrap();

        match stmt {
            Statement::Insert {
                source: InsertSource::Query(_),
                ..
            } => {}
            other => panic!("expected insert query source, got {other:?}"),
        }
    }

    #[test]
    fn parses_statement_variants() {
        assert!(matches!(
            parse("create table users (id integer primary key, name text not null)").unwrap(),
            Statement::CreateTable {
                if_not_exists: false,
                ..
            }
        ));
        assert!(matches!(
            parse("create table if not exists users (id integer primary key)").unwrap(),
            Statement::CreateTable {
                if_not_exists: true,
                ..
            }
        ));
        assert!(matches!(
            parse("drop table users").unwrap(),
            Statement::DropTable {
                if_exists: false,
                ..
            }
        ));
        assert!(matches!(
            parse("drop table if exists users").unwrap(),
            Statement::DropTable {
                if_exists: true,
                ..
            }
        ));
        assert_eq!(
            parse("drop table if exists Users, Orders").unwrap(),
            Statement::DropTable {
                names: vec![qn("users"), qn("orders")],
                if_exists: true,
            }
        );
        assert!(matches!(
            parse("insert into users (id, name) values (1, 'ann')").unwrap(),
            Statement::Insert {
                source: InsertSource::Values(_),
                ..
            }
        ));
        assert!(matches!(
            parse("select * from users").unwrap(),
            Statement::Query(_)
        ));
        assert!(matches!(
            parse("update users set name = 'bob' where id = 1").unwrap(),
            Statement::Update { .. }
        ));
        assert!(matches!(
            parse("delete from users where id = 1").unwrap(),
            Statement::Delete { .. }
        ));
        assert!(matches!(
            parse("explain select * from users").unwrap(),
            Statement::Explain(_)
        ));
        assert!(matches!(
            parse("drop index users_name").unwrap(),
            Statement::DropIndex { .. }
        ));
    }

    #[test]
    fn parses_update_from_and_delete_using() {
        // `UPDATE ... FROM` and `DELETE ... USING` join extra relations into
        // the write target (docs/specs/subqueries.md section 8).
        let Statement::Update { table, from, .. } =
            parse("update users set name = 'x' from accounts where users.id = accounts.id")
                .unwrap()
        else {
            panic!("expected UPDATE");
        };
        assert_eq!(table, "users");
        assert_eq!(from.len(), 1);
        assert!(matches!(from[0], FromItem::Table { .. }));

        let Statement::Delete { table, using, .. } =
            parse("delete from users using accounts where users.id = accounts.id").unwrap()
        else {
            panic!("expected DELETE");
        };
        assert_eq!(table, "users");
        assert_eq!(using.len(), 1);
    }

    #[test]
    fn parses_returning_clause_for_dml() {
        // INSERT ... RETURNING <items>: the projection list is carried on the AST.
        let Statement::Insert { returning, .. } =
            parse("insert into users (id, name) values (1, 'ann') returning id, name").unwrap()
        else {
            panic!("expected insert");
        };
        let returning = returning.expect("returning present");
        assert_eq!(returning.len(), 2);
        assert!(matches!(
            returning[0],
            SelectItem::Expression {
                expr: Expr::ColumnRef { column: ref c, .. },
                ..
            } if c == "id"
        ));

        // UPDATE ... RETURNING * carries a wildcard item.
        let Statement::Update { returning, .. } =
            parse("update users set name = 'bob' where id = 1 returning *").unwrap()
        else {
            panic!("expected update");
        };
        assert!(matches!(returning.as_deref(), Some([SelectItem::Wildcard])));

        // DELETE ... RETURNING <expr AS alias> carries an aliased expression.
        let Statement::Delete { returning, .. } =
            parse("delete from users where id = 1 returning id + 1 as next_id").unwrap()
        else {
            panic!("expected delete");
        };
        assert!(matches!(
            returning.as_deref(),
            Some([SelectItem::Expression { alias: Some(a), .. }]) if a == "next_id"
        ));

        // No RETURNING clause leaves the field None.
        let Statement::Insert { returning, .. } =
            parse("insert into users (id) values (1)").unwrap()
        else {
            panic!("expected insert");
        };
        assert!(returning.is_none());
    }

    #[test]
    fn parses_on_conflict_clause() {
        use crate::{ConflictAction, ConflictTarget};

        // ON CONFLICT DO NOTHING with no target.
        let Statement::Insert { on_conflict, .. } =
            parse("insert into t (id) values (1) on conflict do nothing").unwrap()
        else {
            panic!("expected insert");
        };
        let on_conflict = on_conflict.expect("on_conflict present");
        assert!(on_conflict.target.is_none());
        assert!(matches!(on_conflict.action, ConflictAction::DoNothing));

        // ON CONFLICT (id) DO UPDATE SET ... WHERE, with an excluded reference.
        let Statement::Insert { on_conflict, .. } = parse(
            "insert into t (id, n) values (1, 'a') on conflict (id) \
             do update set n = excluded.n where t.n <> excluded.n",
        )
        .unwrap() else {
            panic!("expected insert");
        };
        let on_conflict = on_conflict.expect("on_conflict present");
        assert!(matches!(
            on_conflict.target,
            Some(ConflictTarget::Columns(ref cols)) if cols == &["id".to_string()]
        ));
        let ConflictAction::DoUpdate {
            assignments,
            filter,
        } = on_conflict.action
        else {
            panic!("expected DO UPDATE");
        };
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].column, "n");
        assert!(filter.is_some());

        // ON CONSTRAINT is rejected as unsupported (no named constraints).
        let err = parse("insert into t (id) values (1) on conflict on constraint t_pk do nothing")
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        // No ON CONFLICT clause leaves the field None.
        let Statement::Insert { on_conflict, .. } = parse("insert into t (id) values (1)").unwrap()
        else {
            panic!("expected insert");
        };
        assert!(on_conflict.is_none());
    }

    #[test]
    fn parses_transaction_control_statements() {
        // BEGIN and its synonyms parse to a `Begin` with no explicit isolation.
        for sql in ["begin", "begin transaction", "start transaction"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Begin { isolation: None },
                "for `{sql}`"
            );
        }
        // COMMIT and END both parse to `Commit`.
        for sql in ["commit", "end"] {
            assert_eq!(parse(sql).unwrap(), Statement::Commit, "for `{sql}`");
        }
        assert_eq!(parse("rollback").unwrap(), Statement::Rollback);
    }

    #[test]
    fn parses_transaction_scoped_isolation_levels() {
        use common::IsolationLevel;

        // BEGIN / START TRANSACTION ISOLATION LEVEL <level> carries the mapped level.
        // READ UNCOMMITTED/READ COMMITTED -> ReadCommitted; REPEATABLE READ/SNAPSHOT
        // -> RepeatableRead; SERIALIZABLE -> Serializable (SSI, `docs/specs/ssi.md`).
        for (sql, level) in [
            (
                "begin isolation level read uncommitted",
                IsolationLevel::ReadCommitted,
            ),
            (
                "begin transaction isolation level read committed",
                IsolationLevel::ReadCommitted,
            ),
            (
                "begin isolation level repeatable read",
                IsolationLevel::RepeatableRead,
            ),
            (
                "start transaction isolation level serializable",
                IsolationLevel::Serializable,
            ),
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Begin {
                    isolation: Some(level)
                },
                "for `{sql}`"
            );
        }

        // SET TRANSACTION ISOLATION LEVEL <level> parses to the transaction-scoped
        // variant with the mapped level.
        for (sql, level) in [
            (
                "set transaction isolation level read committed",
                IsolationLevel::ReadCommitted,
            ),
            (
                "set transaction isolation level serializable",
                IsolationLevel::Serializable,
            ),
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::SetTransaction {
                    isolation: Some(level)
                },
                "for `{sql}`"
            );
        }

        // `READ WRITE` is accepted and ignored (the default access mode).
        assert_eq!(
            parse("begin read write").unwrap(),
            Statement::Begin { isolation: None }
        );
        assert_eq!(
            parse("begin isolation level repeatable read read write").unwrap(),
            Statement::Begin {
                isolation: Some(IsolationLevel::RepeatableRead)
            }
        );
    }

    #[test]
    fn parses_session_characteristics_isolation_levels() {
        use common::IsolationLevel;

        // `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` parses
        // to the session-default variant with the SAME level mapping as
        // BEGIN / SET TRANSACTION (G2 reuses G1's `map_isolation_level`).
        for (sql, level) in [
            (
                "set session characteristics as transaction isolation level read uncommitted",
                IsolationLevel::ReadCommitted,
            ),
            (
                "set session characteristics as transaction isolation level read committed",
                IsolationLevel::ReadCommitted,
            ),
            (
                "set session characteristics as transaction isolation level repeatable read",
                IsolationLevel::RepeatableRead,
            ),
            (
                "set session characteristics as transaction isolation level serializable",
                IsolationLevel::Serializable,
            ),
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::SetSessionCharacteristics {
                    isolation: Some(level)
                },
                "for `{sql}`"
            );
        }

        // `READ WRITE` (the default access mode) is accepted and ignored, yielding a
        // no-isolation-level session-default set (a no-op success at the server).
        assert_eq!(
            parse("set session characteristics as transaction read write").unwrap(),
            Statement::SetSessionCharacteristics { isolation: None }
        );
    }

    #[test]
    fn rejects_unsupported_transaction_control_forms() {
        // sqlparser 0.56's PostgreSQL dialect does not recognize `ABORT`, so it
        // is a syntax error rather than mapping to ROLLBACK; v1 does not add it.
        // READ ONLY (we don't enforce read-only) and chaining are rejected at parse
        // time. (`SET SESSION CHARACTERISTICS` IS supported as of G2; savepoints ARE
        // supported — see `parses_savepoint_statements`.)
        for sql in [
            "abort",
            "start transaction read only",
            "begin read only",
            "commit and chain",
            "rollback and chain",
            "set session characteristics as transaction read only",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
    }

    #[test]
    fn parses_savepoint_statements() {
        assert_eq!(
            parse("savepoint s1").unwrap(),
            Statement::Savepoint {
                name: "s1".to_string()
            }
        );
        // Identifiers are lowercase-normalized.
        assert_eq!(
            parse("SAVEPOINT MyPoint").unwrap(),
            Statement::Savepoint {
                name: "mypoint".to_string()
            }
        );
        assert_eq!(
            parse("release savepoint s1").unwrap(),
            Statement::ReleaseSavepoint {
                name: "s1".to_string()
            }
        );
        // RELEASE without the SAVEPOINT keyword is also accepted.
        assert_eq!(
            parse("release s1").unwrap(),
            Statement::ReleaseSavepoint {
                name: "s1".to_string()
            }
        );
        assert_eq!(
            parse("rollback to savepoint s1").unwrap(),
            Statement::RollbackToSavepoint {
                name: "s1".to_string()
            }
        );
        // `ROLLBACK TO <name>` (no SAVEPOINT keyword) and `ROLLBACK WORK TO`.
        assert_eq!(
            parse("rollback to s1").unwrap(),
            Statement::RollbackToSavepoint {
                name: "s1".to_string()
            }
        );
        // Plain ROLLBACK (no savepoint) still aborts the transaction.
        assert_eq!(parse("rollback").unwrap(), Statement::Rollback);
    }

    #[test]
    fn parses_vacuum_with_optional_table() {
        // sqlparser 0.56 cannot parse VACUUM; it is intercepted before sqlparser.
        // Bare VACUUM (and its trailing-semicolon spelling) targets the whole DB.
        for sql in ["vacuum", "VACUUM", "vacuum;", "  VACUUM ;  "] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Vacuum {
                    table: None,
                    analyze: false,
                },
                "for `{sql}`"
            );
        }
        // VACUUM <table> targets one table; the identifier is lowercase-normalized.
        for sql in ["vacuum users", "VACUUM Users", "vacuum users ;"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Vacuum {
                    table: Some(qn("users")),
                },
                "for `{sql}`"
            );
        }
        // The ANALYZE modifier requests the statistics pass after reclamation
        // (docs/specs/statistics.md §7).
        for sql in ["vacuum analyze", "VACUUM ANALYZE;"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Vacuum {
                    table: None,
                    analyze: true,
                },
                "for `{sql}`"
            );
        }
        for sql in [
            "vacuum analyze users",
            "VACUUM ANALYZE Users;",
            "  vacuum   analyze   users  ; ",
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Vacuum {
                    table: Some(qn("users")),
                },
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn parses_analyze_with_optional_table() {
        // ANALYZE is intercepted before sqlparser like VACUUM
        // (docs/specs/statistics.md §7).
        for sql in ["analyze", "ANALYZE", "analyze;", "  ANALYZE ;  "] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Analyze { table: None },
                "for `{sql}`"
            );
        }
        for sql in ["analyze users", "ANALYZE Users", "analyze users ;"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Analyze {
                    table: Some("users".to_string()),
                },
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn rejects_unsupported_analyze_forms() {
        for sql in [
            "analyze verbose",      // options are unsupported
            "analyze users orders", // multiple tables
            "analyze public.users", // qualified name
            "analyze \"Users\"",    // quoted identifier
            "analyze users (name)", // column list
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
        // `analyzefoo` is not an ANALYZE at all; it falls through to sqlparser.
        assert!(parse("analyzefoo").is_err());
    }

    #[test]
    fn rejects_unsupported_vacuum_forms() {
        // Parenthesized options, multiple tables, quoted names, and a glued
        // keyword argument are all rejected as parse errors.
        for sql in [
            "vacuum (full) users",         // parenthesized options
            "vacuum full",                 // FULL keyword as a bare second token
            "vacuum users orders",         // multiple tables
            "vacuum \"Users\"",            // quoted identifier
            "vacuum analyze users orders", // too many ANALYZE arguments
            "vacuum analyze full",         // unsupported second option
            "vacuum analyze analyze",      // repeated ANALYZE option
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
        // `vacuumfoo` is not a VACUUM at all (no whitespace after the keyword); it
        // falls through to sqlparser, which rejects it as an unknown statement.
        assert!(parse("vacuumfoo").is_err());
    }

    #[test]
    fn parses_truncate_table_forms() {
        for sql in ["truncate users", "TRUNCATE TABLE Users", "truncate users;"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Truncate {
                    tables: vec![qn("users")],
                },
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn parses_multi_table_truncate_and_rejects_duplicate_targets() {
        assert_eq!(
            parse("truncate table Users, Orders").unwrap(),
            Statement::Truncate {
                tables: vec![qn("users"), qn("orders")],
            }
        );

        for sql in [
            "truncate users, USERS",
            "drop table users, USERS",
            "drop table if exists users, users",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
    }

    #[test]
    fn rejects_multi_target_drop_for_non_table_objects() {
        for sql in [
            "drop view first_view, second_view",
            "drop index first_idx, second_idx",
            "drop sequence first_seq, second_seq",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
    }

    #[test]
    fn rejects_unsupported_truncate_forms() {
        for sql in [
            "truncate table only users",
            "truncate users restart identity",
            "truncate users continue identity",
            "truncate users cascade",
            "truncate users restrict",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::FeatureNotSupported, "for `{sql}`");
        }

        let sql = "truncate \"Users\"";
        let err = parse(sql).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
        assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
    }

    #[test]
    fn parses_create_index_forms() {
        assert_eq!(
            parse("create index users_name on users (name)").unwrap(),
            Statement::CreateIndex {
                name: qn("users_name"),
                table: qn("users"),
                columns: vec!["name".to_string()],
                unique: false,
            }
        );
        assert_eq!(
            parse("create unique index uq on users (tenant, name)").unwrap(),
            Statement::CreateIndex {
                name: qn("uq"),
                table: qn("users"),
                columns: vec!["tenant".to_string(), "name".to_string()],
                unique: true,
            }
        );
    }

    #[test]
    fn rejects_unsupported_create_index_forms() {
        // Each form exercises a distinct v1 rejection guard.
        for sql in [
            "create index i on users (name) where id > 0", // partial predicate
            "create index i on users (lower(name))",       // expression column
            "create index i on users (name desc)",         // descending column
            "create index i on users using btree (name)",  // index method
            "create index concurrently i on users (name)", // concurrently
            "create index if not exists i on users (name)", // if not exists
            "create index on users (name)",                // missing index name
        ] {
            assert!(parse(sql).is_err(), "expected `{sql}` to be rejected");
        }
    }

    #[test]
    fn parses_create_and_drop_sequence_forms() {
        assert_eq!(
            parse("create sequence users_id_seq").unwrap(),
            Statement::CreateSequence {
                name: qn("users_id_seq"),
                options: SequenceOptions::default(),
            }
        );
        assert_eq!(
            parse(
                "create sequence s increment by -2 start with 10 minvalue 0 maxvalue 20 cycle cache 5"
            )
            .unwrap(),
            Statement::CreateSequence {
                name: qn("s"),
                options: SequenceOptions {
                    increment: -2,
                    start: Some(10),
                    min_value: Some(0),
                    max_value: Some(20),
                    cycle: true,
                },
            }
        );
        assert_eq!(
            parse("drop sequence if exists s").unwrap(),
            Statement::DropSequence {
                name: qn("s"),
                if_exists: true,
            }
        );
    }

    #[test]
    fn rejects_unsupported_create_sequence_forms() {
        for sql in [
            "create temporary sequence s",
            "create sequence if not exists s",
            "create sequence s as bigint",
            "create sequence s owned by t.id",
            "create sequence s increment by 1 increment by 2",
            "create sequence s cache 1 cache 2",
            "create sequence s cache 0",
            "create sequence s start with 'x'",
        ] {
            assert!(parse(sql).is_err(), "expected `{sql}` to be rejected");
        }
    }

    #[test]
    fn normalizes_unquoted_identifiers_and_rejects_quoted_identifiers() {
        let stmt = parse("select Users.ID as TheID from Users as U").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::ColumnRef {
                    table: Some(ref table),
                    ref column
                },
                ref alias
            } if table == "users" && column == "id" && alias.as_deref() == Some("theid")
        ));
        assert!(matches!(
            select.from[0],
            FromItem::Table {
                ref name,
                alias: Some(ref alias)
            } if name == &qn("users") && alias == "u"
        ));

        let err = parse("select \"id\" from users").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn parses_schema_qualified_table_names() {
        let stmt = parse("select id from public.users").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.from[0],
            FromItem::Table {
                ref name,
                alias: None
            } if name == &QualifiedName { schema: Some("public".to_string()), name: "users".to_string() }
        ));

        let stmt = parse("select c.oid from pg_catalog.pg_class as c").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.from[0],
            FromItem::Table {
                ref name,
                alias: Some(ref alias)
            } if name == &QualifiedName { schema: Some("pg_catalog".to_string()), name: "pg_class".to_string() } && alias == "c"
        ));
    }

    #[test]
    fn parses_schema_ddl_and_qualified_object_targets() {
        assert_eq!(
            parse("create schema if not exists App").unwrap(),
            Statement::CreateSchema {
                name: "app".to_string(),
                if_not_exists: true
            }
        );
        assert_eq!(
            parse("drop schema if exists app restrict").unwrap(),
            Statement::DropSchema {
                name: "app".to_string(),
                if_exists: true
            }
        );
        assert!(parse("drop schema app cascade").is_err());

        let Statement::CreateTable { name, .. } = parse("create table app.users (id int)").unwrap()
        else {
            panic!("expected CREATE TABLE")
        };
        assert_eq!(
            name,
            QualifiedName {
                schema: Some("app".to_string()),
                name: "users".to_string()
            }
        );
        let Statement::AlterTableSetCompression { table, .. } =
            parse("alter table app.users set (compression = none)").unwrap()
        else {
            panic!("expected ALTER TABLE")
        };
        assert_eq!(table.schema.as_deref(), Some("app"));

        for sql in [
            "select * from db.app.users",
            "create table db.app.users (id int)",
            "truncate db.app.users",
            "vacuum db.app.users",
        ] {
            assert!(parse(sql).is_err(), "for `{sql}`");
        }
    }

    #[test]
    fn rejects_too_deep_qualified_table_name() {
        let err = parse("select id from a.b.c").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn preserves_qualified_dml_targets() {
        assert_eq!(
            parse("insert into public.users values (1)").unwrap(),
            Statement::Insert {
                table: QualifiedName {
                    schema: Some("public".to_string()),
                    name: "users".to_string()
                },
                columns: vec![],
                source: InsertSource::Values(vec![vec![Expr::Literal(Value::Integer(1))]]),
                on_conflict: None,
                returning: None,
            }
        );

        assert_eq!(
            parse("update public.users set name = 'Ada'").unwrap(),
            Statement::Update {
                table: QualifiedName {
                    schema: Some("public".to_string()),
                    name: "users".to_string()
                },
                assignments: vec![Assignment {
                    column: "name".to_string(),
                    value: Expr::Literal(Value::Text("Ada".to_string())),
                }],
                from: Vec::new(),
                filter: None,
                returning: None,
            }
        );

        assert_eq!(
            parse("copy public.users to stdout").unwrap(),
            Statement::Copy {
                table: QualifiedName {
                    schema: Some("public".to_string()),
                    name: "users".to_string()
                },
                columns: vec![],
                direction: CopyDirection::To,
                options: CopyOptions::defaults_for(CopyFormat::Text),
            }
        );
    }

    #[test]
    fn parses_select_wildcards_distinctly() {
        let stmt = parse("select *, users.* from users").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(select.columns[0], SelectItem::Wildcard));
        assert!(matches!(
            select.columns[1],
            SelectItem::QualifiedWildcard(ref table) if table == "users"
        ));
    }

    #[test]
    fn parses_join_contract_and_rejects_unsupported_join_forms() {
        let stmt = parse(
            "select * from users left join orders on users.id = orders.user_id \
             right join refunds on orders.id = refunds.order_id",
        )
        .unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.from[0],
            FromItem::Join {
                join_type: JoinType::Right,
                condition: Some(_),
                ..
            }
        ));

        let cross = parse("select * from users cross join orders").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = cross
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.from[0],
            FromItem::Join {
                join_type: JoinType::Cross,
                condition: None,
                ..
            }
        ));

        assert!(parse("select * from users join orders using (id)").is_err());
        assert!(parse("select * from users natural join orders").is_err());
        assert!(
            parse("select * from users cross join orders on users.id = orders.user_id").is_err()
        );
    }

    #[test]
    fn parses_create_table_primary_key_forms() {
        let Statement::CreateTable {
            columns,
            primary_key,
            ..
        } = parse("create table users (id integer primary key, name text)").unwrap()
        else {
            panic!("expected create table");
        };

        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, DataType::Integer);
        assert_eq!(primary_key, vec!["id"]);

        let Statement::CreateTable {
            columns,
            primary_key,
            ..
        } = parse("create table users (id integer not null, org integer, primary key (id, org))")
            .unwrap()
        else {
            panic!("expected create table");
        };

        assert_eq!(columns.len(), 2);
        assert_eq!(primary_key, vec!["id", "org"]);
    }

    #[test]
    fn integer_width_aliases_map_to_integer() {
        // All integer widths share one 64-bit storage type (DataType::Integer),
        // but each reports its declared PostgreSQL wire width: SMALLINT/INT2 =>
        // int2, INTEGER/INT/INT4 => int4, BIGINT/INT8 => int8.
        let Statement::CreateTable { columns, .. } = parse(
            "create table widths (a smallint, b int2, c int4, d int8, e bigint, f integer, g int)",
        )
        .unwrap() else {
            panic!("expected create table");
        };

        let expected = [
            common::PgType::Int2, // smallint
            common::PgType::Int2, // int2
            common::PgType::Int4, // int4
            common::PgType::Int8, // int8
            common::PgType::Int8, // bigint
            common::PgType::Int4, // integer
            common::PgType::Int4, // int
        ];
        assert_eq!(columns.len(), expected.len());
        for (column, pg_type) in columns.iter().zip(expected) {
            assert_eq!(
                column.data_type,
                DataType::Integer,
                "column {}",
                column.name
            );
            assert_eq!(column.pg_type, Some(pg_type), "column {}", column.name);
        }
    }

    #[test]
    fn oid_type_maps_to_integer_with_oid_wire_type() {
        let Statement::CreateTable { columns, .. } =
            parse("create table oids (a oid, b pg_catalog.oid)").unwrap()
        else {
            panic!("expected create table");
        };

        assert_eq!(columns.len(), 2);
        for column in &columns {
            assert_eq!(column.data_type, DataType::Integer);
            assert_eq!(column.pg_type, Some(common::PgType::Oid));
        }

        let Statement::Query(_) = parse("select cast(1 as pg_catalog.oid)").unwrap() else {
            panic!("expected select");
        };
    }

    #[test]
    fn lowers_regclass_casts_to_catalog_lookup() {
        for sql in [
            "select $1::pg_catalog.regclass",
            "select cast($1 as regclass)",
        ] {
            let Statement::Query(Query {
                body: QueryBody::Select(select),
                ..
            }) = parse(sql).unwrap()
            else {
                panic!("expected SELECT for {sql}");
            };
            assert!(matches!(
                &select.columns[0],
                SelectItem::Expression {
                    expr: Expr::Function { name, args, .. },
                    ..
                } if name == "to_regclass" && args.len() == 1
            ));
        }
    }

    #[test]
    fn serial_family_maps_to_integer_serial_columns() {
        let Statement::CreateTable { columns, .. } = parse(
            "create table ids (a serial, b bigserial, c smallserial, d serial2, e serial4, f serial8)",
        )
        .unwrap() else {
            panic!("expected create table");
        };

        // Serial columns store a 64-bit integer but report their serial kind's width.
        let expected = [
            common::PgType::Int4, // serial
            common::PgType::Int8, // bigserial
            common::PgType::Int2, // smallserial
            common::PgType::Int2, // serial2
            common::PgType::Int4, // serial4
            common::PgType::Int8, // serial8
        ];
        assert_eq!(columns.len(), expected.len());
        for (column, pg_type) in columns.iter().zip(expected) {
            assert_eq!(column.data_type, DataType::Integer);
            assert!(!column.nullable);
            assert_eq!(column.default, Some(common::ParsedDefault::Serial));
            assert_eq!(column.pg_type, Some(pg_type), "column {}", column.name);
        }
    }

    #[test]
    fn rejects_serial_with_explicit_default() {
        assert!(parse("create table t (id serial default 7 primary key)").is_err());
    }

    #[test]
    fn varchar_char_lengths_are_parsed() {
        let Statement::CreateTable { columns, .. } =
            parse("create table t (a varchar(10), b char(5), c character(3), d varchar, e text)")
                .unwrap()
        else {
            panic!("expected create table");
        };

        assert_eq!(columns.len(), 5);
        for column in &columns {
            assert_eq!(column.data_type, DataType::Text);
        }
        // Bounded character types carry their length; unbounded VARCHAR and TEXT do not.
        assert_eq!(columns[0].max_length, Some(10));
        assert_eq!(columns[1].max_length, Some(5));
        assert_eq!(columns[2].max_length, Some(3));
        assert_eq!(columns[3].max_length, None);
        assert_eq!(columns[4].max_length, None);
        // The character kind and its length are reported on the wire: varchar =>
        // varchar OID, char/character => bpchar OID, with the declared length.
        assert_eq!(columns[0].pg_type, Some(common::PgType::Varchar(Some(10))));
        assert_eq!(columns[1].pg_type, Some(common::PgType::Bpchar(Some(5))));
        assert_eq!(columns[2].pg_type, Some(common::PgType::Bpchar(Some(3))));
        assert_eq!(columns[3].pg_type, Some(common::PgType::Varchar(None)));
        assert_eq!(columns[4].pg_type, Some(common::PgType::Text));
    }

    #[test]
    fn rejects_zero_length_character_type() {
        assert!(parse("create table t (a varchar(0))").is_err());
    }

    #[test]
    fn declared_pg_types_are_captured_for_scalar_types() {
        let Statement::CreateTable { columns, .. } = parse(
            "create table t (a real, b double precision, c numeric(10, 2), d boolean, \
             e date, f time, g timestamp, h timestamptz, i interval, j bytea, k uuid)",
        )
        .unwrap() else {
            panic!("expected create table");
        };

        let expected = [
            common::PgType::Float4,
            common::PgType::Float8,
            common::PgType::Numeric {
                precision: Some(10),
                scale: 2,
            },
            common::PgType::Bool,
            common::PgType::Date,
            common::PgType::Time,
            common::PgType::Timestamp,
            common::PgType::Timestamptz,
            common::PgType::Interval,
            common::PgType::Bytea,
            common::PgType::Uuid,
        ];
        assert_eq!(columns.len(), expected.len());
        for (column, pg_type) in columns.iter().zip(expected) {
            assert_eq!(column.pg_type, Some(pg_type), "column {}", column.name);
        }
    }

    #[test]
    fn parses_column_default_constants() {
        let Statement::CreateTable { columns, .. } = parse(
            "create table t (id integer primary key, n integer default 7, \
             m integer default -3, s text default 'hi', b boolean default true, \
             x text default null, seq integer default nextval('users_id_seq'))",
        )
        .unwrap() else {
            panic!("expected create table");
        };

        assert_eq!(
            columns[1].default,
            Some(common::ParsedDefault::Const(Value::Integer(7)))
        );
        assert_eq!(
            columns[2].default,
            Some(common::ParsedDefault::Const(Value::Integer(-3)))
        );
        assert_eq!(
            columns[3].default,
            Some(common::ParsedDefault::Const(Value::Text("hi".to_string())))
        );
        assert_eq!(
            columns[4].default,
            Some(common::ParsedDefault::Const(Value::Boolean(true)))
        );
        assert_eq!(
            columns[5].default,
            Some(common::ParsedDefault::Const(Value::Null))
        );
        assert_eq!(
            columns[6].default,
            Some(common::ParsedDefault::Nextval("users_id_seq".to_string()))
        );
        assert_eq!(columns[0].default, None);
    }

    #[test]
    fn parses_unique_constraints() {
        let Statement::CreateTable {
            unique,
            primary_key,
            ..
        } = parse(
            "create table t (id integer primary key, email text unique, \
             a integer, b integer, unique (a, b))",
        )
        .unwrap()
        else {
            panic!("expected create table");
        };

        assert_eq!(primary_key, vec!["id".to_string()]);
        // Column-level UNIQUE on email, then table-level UNIQUE (a, b).
        assert_eq!(
            unique,
            vec![
                vec!["email".to_string()],
                vec!["a".to_string(), "b".to_string()],
            ]
        );
    }

    #[test]
    fn carries_non_constant_default_as_expression_text() {
        // A non-constant default is no longer rejected at parse time: it is carried
        // as canonical SQL text for the binder to bind (and to reject if invalid,
        // e.g. an arithmetic default that references a column).
        let Statement::CreateTable { columns, .. } =
            parse("create table t (a integer, b integer default a + 1)").unwrap()
        else {
            panic!("expected create table");
        };
        assert_eq!(
            columns[1].default,
            Some(common::ParsedDefault::Expr("a + 1".to_string()))
        );

        let Statement::CreateTable { columns, .. } =
            parse("create table t (a timestamp default now())").unwrap()
        else {
            panic!("expected create table");
        };
        assert_eq!(
            columns[0].default,
            Some(common::ParsedDefault::Expr("now()".to_string()))
        );

        // A malformed nextval default (non-string argument) is still a parse error.
        assert!(parse("create table t (a integer default nextval(42))").is_err());
    }

    #[test]
    fn collects_column_and_table_check_constraints_as_text() {
        // Column-level and table-level CHECKs flatten into `checks` as canonical
        // SQL text, in declaration order, for the binder to bind against the table.
        let Statement::CreateTable { checks, .. } = parse(
            "create table t (id integer primary key, n integer check (n > 0), \
             lo integer, hi integer, check (lo <= hi))",
        )
        .unwrap() else {
            panic!("expected create table");
        };
        assert_eq!(checks, vec!["n > 0".to_string(), "lo <= hi".to_string()]);

        // A named CHECK is rejected (only unnamed checks are supported).
        assert!(
            parse("create table t (id integer primary key, constraint c check (id > 0))").is_err()
        );
        assert!(parse("create table t (id integer primary key check (id > 0))").is_ok());
    }

    #[test]
    fn parses_representative_expressions() {
        let stmt = parse(
            "select -id + 2 * 3, not active, name || 'x', id is not null, \
             id in (1, 2), score not between 1 and 9, name not like 'a%', \
             case active when true then 'yes' else 'no' end, \
             case when id = 1 then 'one' end, cast(id as text) \
             from users where deleted is null group by active having count(*) > 0 \
             order by name desc nulls last limit 10 offset 5",
        )
        .unwrap();

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            order_by,
            limit,
            offset,
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::BinaryOp { op: BinOp::Add, .. },
                ..
            }
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::UnaryOp {
                    op: UnaryOp::Not,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            select.columns[2],
            SelectItem::Expression {
                expr: Expr::BinaryOp {
                    op: BinOp::Concat,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            select.columns[7],
            SelectItem::Expression {
                expr: Expr::Case {
                    operand: Some(_),
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            select.columns[8],
            SelectItem::Expression {
                expr: Expr::Case { operand: None, .. },
                ..
            }
        ));
        assert!(matches!(
            select.columns[9],
            SelectItem::Expression {
                expr: Expr::Cast {
                    data_type: DataType::Text,
                    ..
                },
                ..
            }
        ));
        assert_eq!(limit, Some(10));
        assert_eq!(offset, Some(5));
        assert!(!order_by[0].ascending);
        assert_eq!(order_by[0].nulls_first, Some(false));
    }

    #[test]
    fn parses_coalesce_nullif_and_distinct_operators() {
        let stmt = parse(
            "select coalesce(name, 'x'), nullif(id, 0), id is distinct from 1, \
             id is not distinct from 2 from users",
        )
        .unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        // COALESCE / NULLIF parse as ordinary function calls (the binder desugars
        // them to CASE).
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { name, .. },
                ..
            } if name.eq_ignore_ascii_case("coalesce")
        ));
        assert!(matches!(
            &select.columns[1],
            SelectItem::Expression {
                expr: Expr::Function { name, .. },
                ..
            } if name.eq_ignore_ascii_case("nullif")
        ));
        assert!(matches!(
            select.columns[2],
            SelectItem::Expression {
                expr: Expr::BinaryOp {
                    op: BinOp::IsDistinctFrom,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            select.columns[3],
            SelectItem::Expression {
                expr: Expr::BinaryOp {
                    op: BinOp::IsNotDistinctFrom,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn parses_ilike_keeps_case_insensitive_flag() {
        let stmt = parse("select id from users where name ilike 'a%'").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.filter,
            Some(Expr::Like {
                case_insensitive: true,
                negated: false,
                escape: Some('\\'),
                ..
            })
        ));
    }

    #[test]
    fn parses_like_escape_clause() {
        let stmt = parse("select id from users where name like 'x!%' escape '!'").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.filter,
            Some(Expr::Like {
                case_insensitive: false,
                escape: Some('!'),
                ..
            })
        ));

        // `ESCAPE ''` disables escaping.
        let empty = parse("select id from users where name like 'x' escape ''").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = empty
        else {
            panic!("expected select");
        };
        assert!(matches!(
            select.filter,
            Some(Expr::Like { escape: None, .. })
        ));

        // A multi-character ESCAPE is rejected.
        assert!(parse("select id from users where name like 'x' escape 'ab'").is_err());
    }

    #[test]
    fn parses_ceil_floor_into_function_calls() {
        // CEIL/FLOOR are dedicated grammar; CEILING is an ordinary function. All
        // three normalize to `Function` calls.
        for (sql, expected) in [
            ("select ceil(d) from m", "ceil"),
            ("select ceiling(d) from m", "ceiling"),
            ("select floor(d) from m", "floor"),
        ] {
            let Statement::Query(Query {
                body: QueryBody::Select(select),
                ..
            }) = parse(sql).unwrap()
            else {
                panic!("expected select");
            };
            assert!(
                matches!(
                    &select.columns[0],
                    SelectItem::Expression {
                        expr: Expr::Function { name, args, distinct: false },
                        ..
                    } if name == expected && args.len() == 1
                ),
                "for `{sql}`"
            );
        }
        // `CEIL(expr TO field)` is unsupported.
        assert!(parse("select ceil(d to day) from m").is_err());
    }

    #[test]
    fn normalizes_position_into_function_call() {
        let stmt = parse("select position('b' in name) from users").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { name, args, distinct: false },
                ..
            } if name == "position" && args.len() == 2
        ));
    }

    #[test]
    fn normalizes_extract_into_function_call() {
        let stmt = parse("select extract(year from d) from t").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        assert!(matches!(
            &select.columns[0],
            SelectItem::Expression {
                expr: Expr::Function { name, args, distinct: false },
                ..
            } if name == "extract"
                && args.len() == 2
                && matches!(
                    &args[0],
                    FunctionArg::Expr(Expr::Literal(Value::Text(field))) if field == "year"
                )
        ));
        // An unsupported field is rejected.
        assert!(parse("select extract(quarter from d) from t").is_err());
    }

    #[test]
    fn parses_literals() {
        let stmt = parse("select null, true, false, 42, 'text'").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };

        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Literal(Value::Null),
                ..
            }
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::Literal(Value::Boolean(true)),
                ..
            }
        ));
        assert!(matches!(
            select.columns[2],
            SelectItem::Expression {
                expr: Expr::Literal(Value::Boolean(false)),
                ..
            }
        ));
        assert!(matches!(
            select.columns[3],
            SelectItem::Expression {
                expr: Expr::Literal(Value::Integer(42)),
                ..
            }
        ));
        assert!(matches!(
            select.columns[4],
            SelectItem::Expression {
                expr: Expr::Literal(Value::Text(ref text)),
                ..
            } if text == "text"
        ));
    }

    #[test]
    fn parses_parameter_placeholder() {
        let stmt = parse("select id from users where id = $1").unwrap();
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = stmt
        else {
            panic!("expected select");
        };
        let Some(Expr::BinaryOp { right, .. }) = select.filter else {
            panic!("expected filter comparison");
        };
        assert!(matches!(*right, Expr::Placeholder(1)));
    }

    #[test]
    fn rejects_zero_parameter_placeholder() {
        let err = parse("select id from users where id = $0").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
    }

    #[test]
    fn rejects_unsupported_explain_and_create_table_options() {
        let err = parse("explain analyze select * from users").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);

        let err = parse("explain update users set name = 'Ada'").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);

        let err = parse("insert into users values (1) limit 1").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);

        let err = parse("create table users (id integer primary key, org integer primary key)")
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);

        let err =
            parse("create table users (id integer, constraint pk primary key (id))").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);

        let err = parse("create table users (id integer primary key deferrable)").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn parses_create_table_with_compression() {
        for (sql, expected) in [
            (
                "create table t (id integer primary key) with (compression = 'zstd')",
                Some(CompressionSetting::Zstd),
            ),
            (
                "create table t (id integer primary key) with (compression = 'none')",
                Some(CompressionSetting::None),
            ),
            (
                "create table t (id integer primary key) with (compression = zstd)",
                Some(CompressionSetting::Zstd),
            ),
            ("create table t (id integer primary key)", None),
        ] {
            let Statement::CreateTable { compression, .. } = parse(sql).unwrap() else {
                panic!("expected CreateTable for `{sql}`");
            };
            assert_eq!(compression, expected, "for `{sql}`");
        }
    }

    #[test]
    fn accepts_validated_noop_fillfactor() {
        for value in [10, 70, 100] {
            let sql =
                format!("create table t (id integer primary key) with (fillfactor = {value})");
            let Statement::CreateTable {
                compression, toast, ..
            } = parse(&sql).unwrap()
            else {
                panic!("expected CreateTable for `{sql}`");
            };
            assert_eq!(compression, None);
            assert_eq!(toast, ToastOptionPatch::default());
        }
    }

    #[test]
    fn parses_create_table_with_toast_options() {
        let Statement::CreateTable { toast, .. } = parse(
            "create table t (id integer primary key, body text) with \
             (toast = aggressive, toast_tuple_target = 4096, \
              toast_min_value_size = 512, toast_compression = 'zstd_dict')",
        )
        .unwrap() else {
            panic!("expected CreateTable");
        };

        assert_eq!(toast.mode, Some(ToastMode::Aggressive));
        assert_eq!(toast.tuple_target, Some(4096));
        assert_eq!(toast.min_value_size, Some(512));
        assert_eq!(toast.compression, Some(ToastCompression::ZstdDict));
    }

    #[test]
    fn rejects_bad_create_table_options() {
        for sql in [
            "create table t (id integer) with (fillfactor = '70')",
            "create table t (id integer) with (fillfactor = 10.5)",
            "create table t (id integer) with (fillfactor = -10.5)",
            "create table t (id integer) with (fillfactor = 10, fillfactor = 20)",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
        for value in [9, 101] {
            let sql = format!("create table t (id integer) with (fillfactor = {value})");
            let err = parse(&sql).unwrap_err();
            assert_eq!(err.code, SqlState::InvalidParameterValue, "for `{sql}`");
        }
        let err = parse("create table t (id integer) with (fillfactor = -10)").unwrap_err();
        assert_eq!(err.code, SqlState::InvalidParameterValue);
        let err = parse("create table t (id integer) with (fillfactor = 4294967296)").unwrap_err();
        assert_eq!(err.code, SqlState::InvalidParameterValue);
        // Known key, unknown codec: deliberately-unsupported => 0A000.
        let err = parse("create table t (id integer primary key) with (compression = 'lz4')")
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        // Duplicate key.
        let err = parse(
            "create table t (id integer primary key) with (compression = 'zstd', compression = 'none')",
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
        let err = parse("create table t (id integer primary key) with (toast_tuple_target = 100)")
            .unwrap_err();
        assert_eq!(err.code, SqlState::InvalidParameterValue);
        let err = parse("create table t (id integer primary key) with (toast_tuple_target = 9000)")
            .unwrap_err();
        assert_eq!(err.code, SqlState::InvalidParameterValue);
        let err =
            parse("create table t (id integer primary key) with (toast = 'always')").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn parses_alter_table_set_compression() {
        for (sql, compression) in [
            (
                "alter table users set (compression = 'zstd')",
                CompressionSetting::Zstd,
            ),
            (
                "ALTER TABLE Users SET (compression = 'none');",
                CompressionSetting::None,
            ),
            (
                "alter table users set (compression = zstd)",
                CompressionSetting::Zstd,
            ),
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::AlterTableSetCompression {
                    table: qn("users"),
                    compression,
                },
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn parses_alter_table_set_toast_options() {
        let expected = TableOptionPatch {
            compression: Some(CompressionSetting::Zstd),
            toast: ToastOptionPatch {
                mode: Some(ToastMode::Aggressive),
                tuple_target: Some(4096),
                min_value_size: None,
                compression: Some(ToastCompression::Zstd),
            },
        };
        assert_eq!(
            parse(
                "alter table users set (compression = zstd, toast = aggressive, \
                 toast_tuple_target = 4096, toast_compression = zstd)"
            )
            .unwrap(),
            Statement::AlterTableSetOptions {
                table: qn("users"),
                options: expected,
            }
        );
    }

    #[test]
    fn parses_alter_table_primary_key_forms() {
        assert_eq!(
            parse("alter table users add primary key (id)").unwrap(),
            Statement::AlterTableAddPrimaryKey {
                table: qn("users"),
                columns: vec!["id".to_string()],
                constraint_name: None,
            }
        );
        assert_eq!(
            parse("ALTER TABLE ONLY Users ADD CONSTRAINT users_pkey PRIMARY KEY (Id, Tenant);")
                .unwrap(),
            Statement::AlterTableAddPrimaryKey {
                table: qn("users"),
                columns: vec!["id".to_string(), "tenant".to_string()],
                constraint_name: Some("users_pkey".to_string()),
            }
        );
        assert_eq!(
            parse("alter table users drop primary key").unwrap(),
            Statement::AlterTableDropPrimaryKey {
                table: qn("users"),
                constraint_name: None,
            }
        );
        assert_eq!(
            parse("alter table only users drop constraint users_pkey").unwrap(),
            Statement::AlterTableDropPrimaryKey {
                table: qn("users"),
                constraint_name: Some("users_pkey".to_string()),
            }
        );
    }

    #[test]
    fn rejects_unsupported_alter_forms() {
        for sql in [
            "alter index foo set (compression = 'zstd')",
            "alter table users alter column x set not null",
            "alter table users rename constraint old_name to new_name",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Parse, "for `{sql}`");
            assert_eq!(err.code, SqlState::SyntaxError, "for `{sql}`");
        }
        let err = parse("alter table users set (fillfactor = 70)").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
        let err = parse("alter table users set (compression = 'lz4')").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        let err = parse("alter table users set (toast_tuple_target = 100)").unwrap_err();
        assert_eq!(err.code, SqlState::InvalidParameterValue);
        // Malformed (no option list) is a plain syntax error.
        let err = parse("alter table users set compression").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn rejects_conditional_index_ddl() {
        let err = parse("create index if not exists users_name on users (name)").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);

        let err = parse("drop index if exists users_name").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn parses_copy_from_stdin_text_defaults() {
        assert_eq!(
            parse("copy users from stdin").unwrap(),
            Statement::Copy {
                table: qn("users"),
                columns: vec![],
                direction: CopyDirection::From,
                options: CopyOptions::defaults_for(CopyFormat::Text),
            }
        );
    }

    #[test]
    fn accepts_pgbench_copy_freeze_on_as_compatibility_noop() {
        assert_eq!(
            parse("copy pgbench_accounts from stdin with (freeze on)").unwrap(),
            Statement::Copy {
                table: qn("pgbench_accounts"),
                columns: vec![],
                direction: CopyDirection::From,
                options: CopyOptions::defaults_for(CopyFormat::Text),
            }
        );

        let err = parse("copy pgbench_accounts from stdin with (freeze true)").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        let err = parse("copy pgbench_accounts to stdout with (freeze on)").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        for sql in [
            "copy pgbench_accounts from stdin with (format csv, freeze on)",
            "copy pgbench_accounts from stdin with (freeze on, format csv)",
            "copy\npgbench_accounts from stdin with (freeze on)",
            "/* compatibility probe */ copy pgbench_accounts from stdin with (freeze on)",
        ] {
            let err = parse(sql).unwrap_err();
            assert_eq!(err.code, SqlState::FeatureNotSupported, "for {sql}");
        }
    }

    #[test]
    fn parses_copy_to_stdout_with_column_list() {
        let Statement::Copy {
            table,
            columns,
            direction,
            options,
        } = parse("copy users (id, name) to stdout").unwrap()
        else {
            panic!("expected COPY");
        };
        assert_eq!(table, "users");
        assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(direction, CopyDirection::To);
        assert_eq!(options.format, CopyFormat::Text);
    }

    #[test]
    fn parses_copy_csv_with_modern_options() {
        let Statement::Copy { options, .. } =
            parse("copy users from stdin with (format csv, header true, delimiter ';', null 'NA')")
                .unwrap()
        else {
            panic!("expected COPY");
        };
        assert_eq!(options.format, CopyFormat::Csv);
        assert!(options.header);
        assert_eq!(options.delimiter, ';');
        assert_eq!(options.null_string, "NA");
        assert_eq!(options.quote, '"');
    }

    #[test]
    fn parses_copy_legacy_csv_header() {
        let Statement::Copy { options, .. } =
            parse("copy users from stdin with csv header").unwrap()
        else {
            panic!("expected COPY");
        };
        assert_eq!(options.format, CopyFormat::Csv);
        assert!(options.header);
    }

    #[test]
    fn copy_escape_defaults_to_customized_quote() {
        let Statement::Copy { options, .. } =
            parse("copy users from stdin with (format csv, quote '|')").unwrap()
        else {
            panic!("expected COPY");
        };
        assert_eq!(options.quote, '|');
        assert_eq!(options.escape, '|');
    }

    #[test]
    fn rejects_copy_binary_format() {
        let err = parse("copy users from stdin with (format binary)").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn rejects_copy_server_side_file() {
        let err = parse("copy users from '/tmp/data.csv'").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn rejects_copy_query_source() {
        let err = parse("copy (select id from users) to stdout").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn rejects_quote_option_with_text_format() {
        let err = parse("copy users from stdin with (quote '|')").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn rejects_csv_delimiter_equal_to_quote() {
        let err = parse("copy users from stdin with (format csv, delimiter '\"')").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn rejects_newline_delimiter() {
        let err = parse(r"copy users from stdin with (format csv, delimiter E'\n')").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn rejects_backslash_delimiter() {
        let err = parse(r"copy users from stdin with (delimiter '\')").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn rejects_newline_quote_in_csv() {
        let err = parse(r"copy users from stdin with (format csv, quote E'\n')").unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn rejects_escape_option_with_text_format() {
        let err = parse("copy users from stdin with (escape '|')").unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn parses_array_column_types_and_casts() {
        let Statement::CreateTable { columns, .. } = parse("create table t (a integer[])").unwrap()
        else {
            panic!("expected CREATE TABLE");
        };
        assert!(matches!(columns[0].data_type, DataType::Array(_)));
        assert_eq!(
            columns[0].pg_type,
            Some(PgType::array(PgType::Int4).unwrap())
        );

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = parse("select '{}'::text[]").unwrap()
        else {
            panic!("expected SELECT");
        };
        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Cast {
                    data_type: DataType::Array(_),
                    ..
                },
                ..
            }
        ));

        let Statement::CreateTable { columns, .. } =
            parse("create table labels (a varchar(10)[], b char(3)[][])").unwrap()
        else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(columns[0].max_length, Some(10));
        assert_eq!(
            columns[0].pg_type,
            Some(PgType::array(PgType::Varchar(Some(10))).unwrap())
        );
        assert_eq!(columns[1].max_length, Some(3));
        assert_eq!(
            columns[1].pg_type,
            Some(PgType::array(PgType::Bpchar(Some(3))).unwrap())
        );

        let Statement::CreateTable { columns, .. } =
            parse("create table matrix (values integer[][])").unwrap()
        else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(
            columns[0].pg_type,
            Some(PgType::array(PgType::Int4).unwrap())
        );

        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = parse("select ARRAY[[1, 2], [3, 4]]::integer[][]").unwrap()
        else {
            panic!("expected SELECT");
        };
        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::Cast {
                    pg_type: PgType::Array(_),
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn parses_array_constructor_subscripts_and_any() {
        let Statement::Query(Query {
            body: QueryBody::Select(select),
            ..
        }) = parse("select ARRAY[1, 2][1], 2 = ANY(ARRAY[1, 2])").unwrap()
        else {
            panic!("expected SELECT");
        };
        assert!(matches!(
            select.columns[0],
            SelectItem::Expression {
                expr: Expr::ArraySubscript { ref subscripts, .. },
                ..
            } if subscripts.len() == 1
        ));
        assert!(matches!(
            select.columns[1],
            SelectItem::Expression {
                expr: Expr::Any { op: BinOp::Eq, .. },
                ..
            }
        ));
    }

    #[test]
    fn parses_set_returning_functions_in_from() {
        for sql in [
            "select * from unnest(ARRAY[1, 2]) as u(value)",
            "select * from generate_series(1, 3) as g(value)",
        ] {
            let Statement::Query(Query {
                body: QueryBody::Select(select),
                ..
            }) = parse(sql).unwrap()
            else {
                panic!("expected SELECT");
            };
            assert!(
                matches!(
                    select.from.as_slice(),
                    [FromItem::TableFunction {
                        args,
                        alias: Some(_),
                        column_aliases,
                        ..
                    }] if !args.is_empty() && column_aliases == &["value"]
                ),
                "{:?}",
                select.from
            );
        }
    }

    #[test]
    fn rejects_array_slices_and_sized_type_dimensions() {
        assert_eq!(
            parse("select (a)[1:2] from t").unwrap_err().code,
            SqlState::SyntaxError
        );
        assert_eq!(
            parse("create table t (a integer[3])").unwrap_err().code,
            SqlState::SyntaxError
        );
    }
}
