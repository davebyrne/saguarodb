mod ast;
mod convert;

pub use ast::{
    Assignment, BinOp, Expr, FromItem, FunctionArg, InsertSource, JoinType, OrderByItem,
    SelectItem, SelectStatement, Statement, UnaryOp,
};

use common::Result;

pub fn parse(sql: &str) -> Result<Statement> {
    convert::parse_statement(sql)
}

#[cfg(test)]
mod tests {
    use common::{DataType, ErrorKind, SqlState, Value};

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
}
