mod ast;
mod convert;

pub use ast::{
    Assignment, BinOp, Distinct, Expr, FromItem, FunctionArg, InsertSource, JoinType, OrderByItem,
    SelectItem, SelectStatement, Statement, UnaryOp,
};

use common::Result;

pub fn parse(sql: &str) -> Result<Statement> {
    convert::parse_statement(sql)
}

#[cfg(test)]
mod tests {
    use common::{CopyDirection, CopyFormat, CopyOptions, DataType, ErrorKind, SqlState, Value};

    use crate::{
        BinOp, Expr, FromItem, FunctionArg, InsertSource, JoinType, SelectItem, Statement, UnaryOp,
        parse,
    };

    #[test]
    fn rejects_multiple_statements() {
        let err = parse("select 1; select 2").unwrap_err();

        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
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
            Statement::Select(select) => {
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

        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let SelectItem::Expression {
            expr: Expr::Subquery(inner),
            ..
        } = &select.columns[0]
        else {
            panic!("expected scalar subquery, got {:?}", select.columns[0]);
        };
        assert!(
            matches!(inner.from.as_slice(), [FromItem::Table { name, .. }] if name == "accounts")
        );
    }

    #[test]
    fn parses_scalar_subquery_in_where() {
        let stmt =
            parse("select name from users where id = (select min(id) from accounts)").unwrap();

        let Statement::Select(select) = stmt else {
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
            let Statement::Select(select) = parse(sql).unwrap() else {
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
            let Statement::Select(select) = parse(sql).unwrap() else {
                panic!("expected select");
            };
            assert!(
                matches!(select.filter, Some(Expr::Exists { negated: n, .. }) if n == negated),
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn parses_count_star_and_aggregate_distinct_shape() {
        let stmt = parse("select count(*), count(distinct id) from users").unwrap();

        let Statement::Select(select) = stmt else {
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
    fn normalizes_trim_and_substring_into_function_calls() {
        let stmt = parse("select trim(name), substring(name, 2, 3) from users").unwrap();
        let Statement::Select(select) = stmt else {
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
        let Statement::Select(select) = stmt else {
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
            Statement::CreateTable { .. }
        ));
        assert!(matches!(
            parse("drop table users").unwrap(),
            Statement::DropTable { .. }
        ));
        assert!(matches!(
            parse("insert into users (id, name) values (1, 'ann')").unwrap(),
            Statement::Insert {
                source: InsertSource::Values(_),
                ..
            }
        ));
        assert!(matches!(
            parse("select * from users").unwrap(),
            Statement::Select(_)
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
        // The four SQL levels collapse onto two: READ UNCOMMITTED/READ COMMITTED ->
        // ReadCommitted; REPEATABLE READ/SERIALIZABLE -> RepeatableRead (SERIALIZABLE
        // aliases snapshot isolation; we do not implement SSI).
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
                IsolationLevel::RepeatableRead,
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
                IsolationLevel::RepeatableRead,
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
        // to the session-default variant with the SAME four-to-two level mapping as
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
                IsolationLevel::RepeatableRead,
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
                Statement::Vacuum { table: None },
                "for `{sql}`"
            );
        }
        // VACUUM <table> targets one table; the identifier is lowercase-normalized.
        for sql in ["vacuum users", "VACUUM Users", "vacuum users ;"] {
            assert_eq!(
                parse(sql).unwrap(),
                Statement::Vacuum {
                    table: Some("users".to_string()),
                },
                "for `{sql}`"
            );
        }
    }

    #[test]
    fn rejects_unsupported_vacuum_forms() {
        // Parenthesized options, multiple tables, qualified/quoted names, and a glued
        // keyword argument are all rejected as parse errors.
        for sql in [
            "vacuum (full) users",  // parenthesized options
            "vacuum full",          // FULL keyword as a bare second token
            "vacuum users orders",  // multiple tables
            "vacuum public.users",  // qualified name
            "vacuum \"Users\"",     // quoted identifier
            "vacuum analyze users", // ANALYZE keyword
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
    fn parses_create_index_forms() {
        assert_eq!(
            parse("create index users_name on users (name)").unwrap(),
            Statement::CreateIndex {
                name: "users_name".to_string(),
                table: "users".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
            }
        );
        assert_eq!(
            parse("create unique index uq on users (tenant, name)").unwrap(),
            Statement::CreateIndex {
                name: "uq".to_string(),
                table: "users".to_string(),
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
    fn normalizes_unquoted_identifiers_and_rejects_quoted_identifiers() {
        let stmt = parse("select Users.ID as TheID from Users as U").unwrap();
        let Statement::Select(select) = stmt else {
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
            } if name == "users" && alias == "u"
        ));

        let err = parse("select \"id\" from users").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn rejects_schema_qualified_table_name() {
        let err = parse("select id from schema.users").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parse);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn parses_select_wildcards_distinctly() {
        let stmt = parse("select *, users.* from users").unwrap();
        let Statement::Select(select) = stmt else {
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
        let Statement::Select(select) = stmt else {
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
        let Statement::Select(select) = cross else {
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
        // SMALLINT/INT2/INT4/INT8/BIGINT are all 64-bit integers in SaguaroDB
        // (no distinct width); they parse to DataType::Integer like INTEGER/INT.
        let Statement::CreateTable { columns, .. } = parse(
            "create table widths (a smallint, b int2, c int4, d int8, e bigint, f integer, g int)",
        )
        .unwrap() else {
            panic!("expected create table");
        };

        assert_eq!(columns.len(), 7);
        for column in &columns {
            assert_eq!(
                column.data_type,
                DataType::Integer,
                "column {} should map to Integer",
                column.name
            );
        }
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
    }

    #[test]
    fn rejects_zero_length_character_type() {
        assert!(parse("create table t (a varchar(0))").is_err());
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

        let Statement::Select(select) = stmt else {
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
        assert_eq!(select.limit, Some(10));
        assert_eq!(select.offset, Some(5));
        assert!(!select.order_by[0].ascending);
        assert_eq!(select.order_by[0].nulls_first, Some(false));
    }

    #[test]
    fn parses_literals() {
        let stmt = parse("select null, true, false, 42, 'text'").unwrap();
        let Statement::Select(select) = stmt else {
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
        let Statement::Select(select) = stmt else {
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

        let err = parse("create table users (id integer) with (fillfactor = 70)").unwrap_err();
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
    fn parses_copy_from_stdin_text_defaults() {
        assert_eq!(
            parse("copy users from stdin").unwrap(),
            Statement::Copy {
                table: "users".to_string(),
                columns: vec![],
                direction: CopyDirection::From,
                options: CopyOptions::defaults_for(CopyFormat::Text),
            }
        );
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
}
