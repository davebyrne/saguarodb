mod support;

use std::path::Path;

use support::{Connection, TestServer};
use wal::{FileWalManager, WalManager, WalRecordKind};

/// A column `DEFAULT` is applied when an `INSERT` omits the column, including for
/// a `NOT NULL` column with a non-NULL default.
#[tokio::test]
async fn column_default_applied_on_omitted_column() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table t (id integer primary key, n integer default 7, \
             s text not null default 'x', maybe text default null)",
        )
        .await
        .unwrap();

    // Omit n, s, and maybe: all take their defaults.
    server
        .simple_query("insert into t (id) values (1)")
        .await
        .unwrap();
    // Explicitly provide a value: it overrides the default.
    server
        .simple_query("insert into t (id, n) values (2, 99)")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, n, s, maybe from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("7".to_string()),
                Some("x".to_string()),
                None,
            ],
            vec![
                Some("2".to_string()),
                Some("99".to_string()),
                Some("x".to_string()),
                None,
            ],
        ]
    );
}

/// Omitting a `NOT NULL` column that has no default is still rejected.
#[tokio::test]
async fn omitting_not_null_without_default_is_rejected() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, name text not null)")
        .await
        .unwrap();

    let err = server
        .simple_query("insert into t (id) values (1)")
        .await
        .err()
        .expect("statement should fail");
    assert!(
        err.message.contains("23502"),
        "expected NotNullViolation: {}",
        err.message
    );
}

/// A `DEFAULT` whose constant type does not match the column type is rejected at
/// `CREATE TABLE`.
#[tokio::test]
async fn default_type_mismatch_is_rejected() {
    let server = TestServer::start().await.unwrap();
    let err = server
        .simple_query("create table t (id integer primary key, n integer default 'oops')")
        .await
        .err()
        .expect("statement should fail");
    assert!(
        err.message.contains("42804"),
        "expected DatatypeMismatch: {}",
        err.message
    );
}

#[tokio::test]
async fn conditional_table_ddl_is_noop_only_for_existence_conflicts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, note) values (1, 'kept')")
        .await
        .unwrap();

    let duplicate = server
        .simple_query("create table t (id integer primary key)")
        .await
        .err()
        .expect("duplicate create without IF NOT EXISTS should fail");
    assert!(
        duplicate.message.contains("42P07"),
        "expected DuplicateTable: {}",
        duplicate.message
    );

    server
        .simple_query("create table if not exists t (id integer primary key, other text)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, note from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("kept".to_string())]]
    );

    let invalid = server
        .simple_query(
            "create table if not exists t (id integer primary key, n integer default 'oops')",
        )
        .await
        .err()
        .expect("IF NOT EXISTS still validates the table definition");
    assert!(
        invalid.message.contains("42804"),
        "expected DatatypeMismatch for invalid default: {}",
        invalid.message
    );

    let invalid = server
        .simple_query("create table if not exists t (id integer primary key, id text)")
        .await
        .err()
        .expect("IF NOT EXISTS still validates duplicate columns");
    assert!(
        invalid.message.contains("42601"),
        "expected SyntaxError for duplicate column: {}",
        invalid.message
    );

    let invalid = server
        .simple_query(
            "create table if not exists t (id integer primary key, email text, unique (missing))",
        )
        .await
        .err()
        .expect("IF NOT EXISTS still validates UNIQUE columns");
    assert!(
        invalid.message.contains("42703"),
        "expected UndefinedColumn for missing UNIQUE column: {}",
        invalid.message
    );

    server
        .simple_query("drop table if exists missing")
        .await
        .unwrap();
    server.simple_query("drop table if exists t").await.unwrap();

    let missing = server
        .simple_query("select id from t")
        .await
        .err()
        .expect("table should have been dropped");
    assert!(
        missing.message.contains("42P01"),
        "expected UndefinedTable: {}",
        missing.message
    );
}

#[tokio::test]
async fn drop_table_accepts_pgbench_cleanup_list() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query(
            "drop table if exists pgbench_accounts, pgbench_branches, \
             pgbench_history, pgbench_tellers",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn create_table_accepts_pgbench_schema_with_fillfactor() {
    let server = TestServer::start().await.unwrap();
    for sql in [
        "create table pgbench_history (tid int, bid int, aid bigint, delta int, \
         mtime timestamp, filler char(22)) with (fillfactor = 70)",
        "create table pgbench_tellers (tid int not null, bid int, tbalance int, \
         filler char(84)) with (fillfactor = 70)",
        "create table pgbench_accounts (aid bigint not null, bid int, abalance int, \
         filler char(84)) with (fillfactor = 70)",
        "create table pgbench_branches (bid int not null, bbalance int, \
         filler char(88)) with (fillfactor = 70)",
    ] {
        server.simple_query(sql).await.unwrap();
    }

    for table in [
        "pgbench_accounts",
        "pgbench_branches",
        "pgbench_history",
        "pgbench_tellers",
    ] {
        assert!(
            server
                .simple_query(&format!("select * from {table}"))
                .await
                .unwrap()
                .unwrap_rows()
                .is_empty()
        );
    }

    server
        .simple_query(
            "truncate table pgbench_accounts, pgbench_branches, \
             pgbench_history, pgbench_tellers",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn drop_table_removes_multiple_tables_in_one_statement() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table first_table (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table second_table (id integer primary key)")
        .await
        .unwrap();

    server
        .simple_query("drop table first_table, second_table")
        .await
        .unwrap();

    for table in ["first_table", "second_table"] {
        let err = server
            .simple_query(&format!("select id from {table}"))
            .await
            .err()
            .expect("dropped table should not be queryable");
        assert!(
            err.message.contains("42P01"),
            "expected UndefinedTable for {table}: {}",
            err.message
        );
    }
}

#[tokio::test]
async fn drop_table_if_exists_skips_missing_targets_within_the_list() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table first_present (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table second_present (id integer primary key)")
        .await
        .unwrap();

    server
        .simple_query("drop table if exists first_present, missing_between, second_present")
        .await
        .unwrap();

    for table in ["first_present", "second_present"] {
        let err = server
            .simple_query(&format!("select id from {table}"))
            .await
            .err()
            .expect("present target should have been dropped");
        assert!(
            err.message.contains("42P01"),
            "expected UndefinedTable for {table}: {}",
            err.message
        );
    }
}

#[tokio::test]
async fn drop_table_validates_every_target_before_dropping_any() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table kept (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("insert into kept values (1)")
        .await
        .unwrap();
    server
        .simple_query("create view not_a_table as select id from kept")
        .await
        .unwrap();

    let err = server
        .simple_query("drop table if exists kept, not_a_table")
        .await
        .err()
        .expect("a view in the target list should reject the statement");
    assert!(
        err.message.contains("42809"),
        "expected WrongObjectType: {}",
        err.message
    );

    let rows = server
        .simple_query("select id from kept")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn conditional_table_ddl_noops_do_not_log_logical_ddl_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, code integer unique)")
            .await
            .unwrap();
        server
            .simple_query(
                "create table if not exists t (id integer primary key, code integer unique)",
            )
            .await
            .unwrap();
        server
            .simple_query("drop table if exists missing")
            .await
            .unwrap();
    }

    let wal = FileWalManager::open(path.join("wal.dat")).unwrap();
    let records = wal
        .replay_from(0)
        .unwrap()
        .collect::<common::Result<Vec<_>>>()
        .unwrap();
    let create_table_records = records
        .iter()
        .filter(|record| matches!(record.kind, WalRecordKind::CreateTable { .. }))
        .count();
    let drop_table_records = records
        .iter()
        .filter(|record| matches!(record.kind, WalRecordKind::DropTable { .. }))
        .count();
    let create_index_records = records
        .iter()
        .filter(|record| matches!(record.kind, WalRecordKind::CreateIndex { .. }))
        .count();

    assert_eq!(create_table_records, 1);
    assert_eq!(drop_table_records, 0);
    assert_eq!(create_index_records, 2);
}

/// Column defaults persist across a restart (replayed from the durable catalog /
/// `CreateTable` WAL record) and are still applied to inserts after recovery.
#[tokio::test]
async fn column_default_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, n integer default 5)")
            .await
            .unwrap();
        server
            .simple_query("insert into t (id) values (1)")
            .await
            .unwrap();
        // No checkpoint: force recovery to replay the CreateTable WAL record.
    }

    let server = restart(&path).await;
    // A pre-restart default value was persisted with the row.
    let rows = server
        .simple_query("select n from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("5".to_string())]]);

    // The default metadata also survived: a new insert still defaults n.
    server
        .simple_query("insert into t (id) values (2)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select n from t where id = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("5".to_string())]]);
}

/// A non-constant expression `DEFAULT` (a scalar function, arithmetic) is
/// evaluated when the column is omitted; an explicit value still overrides it.
#[tokio::test]
async fn expression_default_applied_on_omitted_column() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table t (id integer primary key, \
             n integer default 2 * 3, s text not null default upper('hi'))",
        )
        .await
        .unwrap();

    server
        .simple_query("insert into t (id) values (1)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, n, s) values (2, 10, 'x')")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, n, s from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("6".to_string()),
                Some("HI".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("10".to_string()),
                Some("x".to_string()),
            ],
        ]
    );
}

/// Invalid expression `DEFAULT`s are rejected at `CREATE TABLE`: one referencing a
/// table column (a default is bound in an empty scope), one whose result type does
/// not match the column, and one calling an unknown function.
#[tokio::test]
async fn expression_default_invalid_is_rejected() {
    let server = TestServer::start().await.unwrap();

    let err = server
        .simple_query("create table t (a integer primary key, b integer default a + 1)")
        .await
        .err()
        .expect("column-referencing default should fail");
    assert!(
        err.message.contains("42703"),
        "expected UndefinedColumn: {}",
        err.message
    );

    let err = server
        .simple_query("create table t (id integer primary key, n integer default upper('x'))")
        .await
        .err()
        .expect("type-mismatched default should fail");
    assert!(
        err.message.contains("42804"),
        "expected DatatypeMismatch: {}",
        err.message
    );

    let err = server
        .simple_query("create table t (id integer primary key, n integer default bogus_fn())")
        .await
        .err()
        .expect("unknown-function default should fail");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError: {}",
        err.message
    );
}

/// An expression `DEFAULT` survives a restart (replayed from the durable catalog /
/// `CreateTable` WAL record) and is still evaluated for inserts after recovery.
#[tokio::test]
async fn expression_default_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, n integer default 3 + 4)")
            .await
            .unwrap();
        server
            .simple_query("insert into t (id) values (1)")
            .await
            .unwrap();
        // No checkpoint: force recovery to replay the CreateTable WAL record.
    }

    let server = restart(&path).await;
    let rows = server
        .simple_query("select n from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("7".to_string())]]);

    // The default metadata also survived: a new insert still evaluates it.
    server
        .simple_query("insert into t (id) values (2)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select n from t where id = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("7".to_string())]]);
}

/// A `CHECK` constraint — both column-level (`n INT CHECK (n > 0)`) and table-level
/// (`CHECK (a <= b)`) — rejects a violating `INSERT` with `SqlState::CheckViolation`
/// (`23514`) and admits a conforming row.
#[tokio::test]
async fn check_constraint_enforced_on_insert() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table t (id integer primary key, n integer check (n > 0), \
             lo integer, hi integer, check (lo <= hi))",
        )
        .await
        .unwrap();

    // A conforming row is accepted.
    server
        .simple_query("insert into t (id, n, lo, hi) values (1, 5, 2, 9)")
        .await
        .unwrap();

    // The column-level check (n > 0) rejects n = 0.
    let err = server
        .simple_query("insert into t (id, n, lo, hi) values (2, 0, 2, 9)")
        .await
        .err()
        .expect("column check violation should fail");
    assert!(
        err.message.contains("23514"),
        "expected CheckViolation: {}",
        err.message
    );

    // The table-level check (lo <= hi) rejects lo > hi.
    let err = server
        .simple_query("insert into t (id, n, lo, hi) values (3, 5, 9, 2)")
        .await
        .err()
        .expect("table check violation should fail");
    assert!(
        err.message.contains("23514"),
        "expected CheckViolation: {}",
        err.message
    );

    let rows = server
        .simple_query("select id from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

/// A `CHECK` that evaluates to `NULL` (unknown) passes, matching PostgreSQL's
/// three-valued semantics: only an explicit `false` violates.
#[tokio::test]
async fn check_constraint_null_operand_passes() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, n integer check (n > 0))")
        .await
        .unwrap();

    // n is NULL, so `n > 0` is NULL (not false): the row is admitted.
    server
        .simple_query("insert into t (id, n) values (1, null)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, n from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string()), None]]);
}

/// A `CHECK` constraint is enforced on `UPDATE`: an assignment that would make the
/// row violate the check is rejected, and the row is unchanged.
#[tokio::test]
async fn check_constraint_enforced_on_update() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, n integer check (n > 0))")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, n) values (1, 5)")
        .await
        .unwrap();

    let err = server
        .simple_query("update t set n = -1 where id = 1")
        .await
        .err()
        .expect("update violating the check should fail");
    assert!(
        err.message.contains("23514"),
        "expected CheckViolation: {}",
        err.message
    );

    // The row is unchanged; a conforming update succeeds.
    server
        .simple_query("update t set n = 10 where id = 1")
        .await
        .unwrap();
    let rows = server
        .simple_query("select n from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("10".to_string())]]);
}

/// A `CHECK` is enforced when `INSERT ... ON CONFLICT DO UPDATE` produces the new
/// row: an upsert whose resulting row violates the check is rejected.
#[tokio::test]
async fn check_constraint_enforced_on_upsert() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, n integer check (n > 0))")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, n) values (1, 5)")
        .await
        .unwrap();

    let err = server
        .simple_query(
            "insert into t (id, n) values (1, 5) \
             on conflict (id) do update set n = -1",
        )
        .await
        .err()
        .expect("upsert producing a violating row should fail");
    assert!(
        err.message.contains("23514"),
        "expected CheckViolation: {}",
        err.message
    );
}

/// Invalid `CHECK` expressions are rejected at `CREATE TABLE`: a non-boolean result,
/// a reference to an unknown column, and an aggregate (not allowed in a constraint).
#[tokio::test]
async fn check_constraint_invalid_at_create() {
    let server = TestServer::start().await.unwrap();

    let err = server
        .simple_query("create table t (id integer primary key, n integer check (n + 1))")
        .await
        .err()
        .expect("non-boolean check should fail");
    assert!(
        err.message.contains("42804"),
        "expected DatatypeMismatch: {}",
        err.message
    );

    let err = server
        .simple_query("create table t (id integer primary key, n integer check (missing > 0))")
        .await
        .err()
        .expect("check referencing an unknown column should fail");
    assert!(
        err.message.contains("42703"),
        "expected UndefinedColumn: {}",
        err.message
    );

    let err = server
        .simple_query("create table t (id integer primary key, n integer check (count(*) > 0))")
        .await
        .err()
        .expect("aggregate in a check should fail");
    assert!(
        err.message.contains("0A000"),
        "expected FeatureNotSupported: {}",
        err.message
    );

    let err = server
        .simple_query("create table qualified_check (id integer, check (qualified_check.id > 0))")
        .await
        .err()
        .expect("table-qualified check references should fail");
    assert!(
        err.message.contains("0A000"),
        "expected FeatureNotSupported: {}",
        err.message
    );
}

#[tokio::test]
async fn rename_table_with_check_constraint_keeps_constraint_enforced() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, check (id > 0))")
        .await
        .unwrap();

    server
        .simple_query("alter table users rename to accounts")
        .await
        .unwrap();
    server
        .simple_query("insert into accounts (id) values (1)")
        .await
        .unwrap();

    let err = server
        .simple_query("insert into accounts (id) values (0)")
        .await
        .err()
        .expect("renamed table should keep enforcing CHECK constraints");
    assert!(
        err.message.contains("23514"),
        "expected CheckViolation: {}",
        err.message
    );
}

/// A `CHECK` constraint survives a restart (replayed from the durable catalog /
/// `CreateTable` WAL record) and is still enforced for inserts after recovery.
#[tokio::test]
async fn check_constraint_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, n integer check (n > 0))")
            .await
            .unwrap();
        server
            .simple_query("insert into t (id, n) values (1, 5)")
            .await
            .unwrap();
        // No checkpoint: force recovery to replay the CreateTable WAL record.
    }

    let server = restart(&path).await;
    // The check metadata survived: a violating insert is still rejected...
    let err = server
        .simple_query("insert into t (id, n) values (2, 0)")
        .await
        .err()
        .expect("check should still be enforced after restart");
    assert!(
        err.message.contains("23514"),
        "expected CheckViolation: {}",
        err.message
    );
    // ...and a conforming insert still succeeds.
    server
        .simple_query("insert into t (id, n) values (3, 7)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("3".to_string())]]
    );
}

#[tokio::test]
async fn sequence_ddl_create_drop_and_if_exists() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create sequence users_id_seq")
        .await
        .unwrap();

    let duplicate = server
        .simple_query("create sequence users_id_seq")
        .await
        .err()
        .expect("duplicate sequence should fail");
    assert!(
        duplicate.message.contains("42P07"),
        "expected DuplicateTable: {}",
        duplicate.message
    );

    server
        .simple_query("drop sequence users_id_seq")
        .await
        .unwrap();
    server
        .simple_query("drop sequence if exists users_id_seq")
        .await
        .unwrap();

    let missing = server
        .simple_query("drop sequence users_id_seq")
        .await
        .err()
        .expect("missing sequence should fail without IF EXISTS");
    assert!(
        missing.message.contains("42P01"),
        "expected UndefinedTable: {}",
        missing.message
    );
}

#[tokio::test]
async fn invalid_sequence_definition_uses_invalid_parameter_value() {
    let server = TestServer::start().await.unwrap();

    let err = server
        .simple_query("create sequence bad_seq increment by 0")
        .await
        .err()
        .expect("invalid sequence definition should fail");
    assert!(
        err.message.contains("22023"),
        "expected InvalidParameterValue: {}",
        err.message
    );
}

#[tokio::test]
async fn drop_sequence_referenced_by_default_is_rejected() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create sequence users_id_seq")
        .await
        .unwrap();
    server
        .simple_query(
            "create table users (id integer primary key default nextval('users_id_seq'), name text)",
        )
        .await
        .unwrap();

    let err = server
        .simple_query("drop sequence users_id_seq")
        .await
        .err()
        .expect("referenced sequence drop should fail");
    assert!(
        err.message.contains("2BP01"),
        "expected DependentObjectsStillExist: {}",
        err.message
    );

    server
        .simple_query("insert into users (name) values ('Ada')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn serial_creates_owned_sequence_and_drop_table_removes_it() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id serial primary key, name text)")
        .await
        .unwrap();
    assert_eq!(
        server
            .simple_query("insert into users (name) values ('Ada') returning id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        server
            .simple_query("insert into users (name) values ('Grace') returning id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("2".to_string())]]
    );

    let err = server
        .simple_query("drop sequence users_id_seq")
        .await
        .err()
        .expect("owned sequence drop should fail");
    assert!(
        err.message.contains("2BP01"),
        "expected DependentObjectsStillExist: {}",
        err.message
    );

    server.simple_query("drop table users").await.unwrap();
    let err = server
        .simple_query("drop sequence users_id_seq")
        .await
        .err()
        .expect("owned sequence should be gone after DROP TABLE");
    assert!(
        err.message.contains("42P01"),
        "expected UndefinedTable: {}",
        err.message
    );
}

#[tokio::test]
async fn multiple_serial_columns_at_non_zero_indices_get_independent_sequences() {
    let server = TestServer::start().await.unwrap();

    // A non-serial column first, then two SERIAL columns at indices 1 and 2. Each is
    // derived straight from its own column marker and gets its own owned sequence —
    // exercising the derive-from-columns path for serials past index 0.
    server
        .simple_query("create table t (name text, a serial, b serial primary key)")
        .await
        .unwrap();
    assert_eq!(
        server
            .simple_query("insert into t (name) values ('x') returning a, b")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string()), Some("1".to_string())]]
    );
    assert_eq!(
        server
            .simple_query("insert into t (name) values ('y') returning a, b")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("2".to_string()), Some("2".to_string())]]
    );

    // Each SERIAL owns a distinct, independently-named sequence.
    for seq in ["t_a_seq", "t_b_seq"] {
        let err = server
            .simple_query(&format!("drop sequence {seq}"))
            .await
            .err()
            .unwrap_or_else(|| panic!("owned sequence {seq} drop should fail"));
        assert!(
            err.message.contains("2BP01"),
            "expected DependentObjectsStillExist for {seq}: {}",
            err.message
        );
    }
}

#[tokio::test]
async fn serial_uses_suffixed_sequence_name_when_default_collides() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create sequence users_id_seq")
        .await
        .unwrap();
    server
        .simple_query("create table users (id serial primary key, name text)")
        .await
        .unwrap();
    assert_eq!(
        server
            .simple_query("insert into users (name) values ('Ada') returning id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]]
    );
    // The explicitly created sequence was not borrowed by the SERIAL default.
    assert_eq!(
        server
            .simple_query("select nextval('users_id_seq') from users")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]]
    );
    server
        .simple_query("drop sequence users_id_seq")
        .await
        .unwrap();
    assert_eq!(
        server
            .simple_query("insert into users (name) values ('Grace') returning id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("2".to_string())]]
    );
}

#[tokio::test]
async fn prepared_serial_chooses_sequence_name_at_execute_time() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let prepare = conn
        .prepare(
            "create_users",
            "create table users (id serial primary key, name text)",
        )
        .await
        .unwrap();
    assert!(prepare.result.is_ok(), "prepare failed");

    assert!(
        conn.ok("create sequence users_id_seq").await.result.is_ok(),
        "create sequence failed"
    );
    let create = conn.execute_prepared("create_users").await.unwrap();
    assert!(
        create.result.is_ok(),
        "prepared create failed: {:?}",
        create.result.err()
    );
    assert_eq!(
        conn.query("insert into users (name) values ('Ada') returning id")
            .await
            .unwrap()
            .rows(),
        vec![vec![Some("1".to_string())]]
    );
    // The explicitly created sequence was not borrowed by the prepared SERIAL DDL.
    assert_eq!(
        conn.query("select nextval('users_id_seq') from users")
            .await
            .unwrap()
            .rows(),
        vec![vec![Some("1".to_string())]]
    );
}

#[tokio::test]
async fn duplicate_column_name_with_serial_is_a_normal_ddl_error() {
    let server = TestServer::start().await.unwrap();

    let err = server
        .simple_query("create table users (id integer, id serial, primary key (id))")
        .await
        .err()
        .expect("duplicate column should fail");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError, not an internal error: {}",
        err.message
    );
}

#[tokio::test]
async fn explicit_default_cannot_borrow_serial_owned_sequence() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id serial primary key)")
        .await
        .unwrap();
    let err = server
        .simple_query("create table posts (id integer primary key default nextval('users_id_seq'))")
        .await
        .err()
        .expect("explicit default should not borrow owned sequence");
    assert!(
        err.message.contains("2BP01"),
        "expected DependentObjectsStillExist: {}",
        err.message
    );
}

#[tokio::test]
async fn sequence_ddl_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query(
                "create sequence users_id_seq increment by 3 start with 7 minvalue 1 maxvalue 99 cycle",
            )
            .await
            .unwrap();
        // No checkpoint: force recovery to replay the CreateSequence WAL record.
    }

    let server = restart(&path).await;
    server
        .simple_query("drop sequence users_id_seq")
        .await
        .unwrap();
    let missing = server
        .simple_query("drop sequence users_id_seq")
        .await
        .err()
        .expect("drop after recovery should have removed the sequence");
    assert!(
        missing.message.contains("42P01"),
        "expected UndefinedTable: {}",
        missing.message
    );
}

#[tokio::test]
async fn sequence_drop_replay_removes_checkpointed_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create sequence users_id_seq")
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
        server
            .simple_query("drop sequence users_id_seq")
            .await
            .unwrap();
        // No checkpoint after the drop: force recovery to replay DropSequence.
    }

    let server = restart(&path).await;
    let missing = server
        .simple_query("drop sequence users_id_seq")
        .await
        .err()
        .expect("DropSequence replay should remove the checkpointed sequence");
    assert!(
        missing.message.contains("42P01"),
        "expected UndefinedTable: {}",
        missing.message
    );
}

#[tokio::test]
async fn prepared_drop_sequence_if_exists_resolves_at_execute_time() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let prepare = conn
        .prepare("drop_seq", "drop sequence if exists users_id_seq")
        .await
        .unwrap();
    assert!(prepare.result.is_ok(), "prepare failed");

    assert!(
        conn.ok("create sequence users_id_seq").await.result.is_ok(),
        "create sequence failed"
    );
    let drop = conn.execute_prepared("drop_seq").await.unwrap();
    assert!(drop.result.is_ok(), "execute failed");

    let missing = conn
        .query("drop sequence users_id_seq")
        .await
        .unwrap()
        .result
        .err()
        .expect("prepared drop should have removed the sequence");
    assert!(
        missing.message.contains("42P01"),
        "expected UndefinedTable: {}",
        missing.message
    );
}

#[tokio::test]
async fn prepared_currval_errors_after_sequence_drop() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    assert!(
        conn.ok("create sequence users_id_seq").await.result.is_ok(),
        "create sequence failed"
    );
    assert!(
        conn.ok("create table seq_probe (id integer primary key)")
            .await
            .result
            .is_ok(),
        "create probe table failed"
    );
    assert!(
        conn.ok("insert into seq_probe (id) values (1)")
            .await
            .result
            .is_ok(),
        "insert probe row failed"
    );
    let prepare = conn
        .prepare(
            "current_seq",
            "select currval('users_id_seq') from seq_probe",
        )
        .await
        .unwrap();
    assert!(prepare.result.is_ok(), "prepare failed");
    assert!(
        conn.ok("select nextval('users_id_seq') from seq_probe")
            .await
            .result
            .is_ok(),
        "nextval failed"
    );
    let first = conn.execute_prepared("current_seq").await.unwrap();
    assert_eq!(
        first.result.unwrap().unwrap_rows(),
        vec![vec![Some("1".to_string())]]
    );

    assert!(
        conn.ok("drop sequence users_id_seq").await.result.is_ok(),
        "drop sequence failed"
    );
    let err = conn
        .execute_prepared("current_seq")
        .await
        .unwrap()
        .result
        .err()
        .expect("prepared currval should fail after the sequence is dropped");
    assert!(
        err.message.contains("0A000"),
        "expected cached-plan reprepare error: {}",
        err.message
    );
}

#[tokio::test]
async fn sequence_ddl_is_transactional() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("begin").await;
    conn.ok("create sequence users_id_seq").await;
    conn.ok("rollback").await;
    let missing = conn.ok("select nextval('users_id_seq')").await;
    assert!(missing.result.is_err());
    assert_eq!(missing.status, b'I');

    conn.ok("begin").await;
    conn.ok("create sequence users_id_seq").await;
    conn.ok("commit").await;
    conn.ok("select nextval('users_id_seq')").await;
}

#[tokio::test]
async fn alter_column_type_rewrites_rows_and_indexes_transactionally() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table typed (id integer primary key, value integer)")
        .await;
    conn.ok("create index typed_value_idx on typed (value)")
        .await;
    conn.ok("insert into typed values (1, 42), (2, null)").await;
    conn.ok("begin").await;
    conn.ok("alter table typed alter column value type text")
        .await;
    assert_eq!(
        conn.ok("select value from typed where value = '42'")
            .await
            .rows(),
        vec![vec![Some("42".to_string())]]
    );
    conn.ok("rollback").await;
    assert_eq!(
        conn.ok("select value from typed where value = 42")
            .await
            .rows(),
        vec![vec![Some("42".to_string())]]
    );

    conn.ok("alter table typed alter value set data type text")
        .await;
    assert_eq!(
        conn.ok("select id from typed where value = '42'")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]]
    );
}

#[tokio::test]
async fn alter_column_type_conversion_failure_is_atomic() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table typed_failure (id integer primary key, value text)")
        .await;
    conn.ok("insert into typed_failure values (1, 'not-an-integer')")
        .await;
    let outcome = conn
        .ok("alter table typed_failure alter column value type integer")
        .await;
    assert!(outcome.result.is_err());
    assert_eq!(
        conn.ok("select value from typed_failure").await.rows(),
        vec![vec![Some("not-an-integer".to_string())]]
    );
}

#[tokio::test]
async fn alter_column_type_validates_defaults_and_identical_type_is_noop() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table typed_default (id integer primary key, value text default 'toolong')")
        .await;
    let rejected = conn
        .ok("alter table typed_default alter column value type varchar(3)")
        .await;
    assert!(rejected.result.is_err());

    conn.prepare("typed_read", "select id from typed_default")
        .await
        .unwrap()
        .unwrap();
    conn.ok("alter table typed_default alter column id type integer")
        .await;
    assert!(
        conn.execute_prepared("typed_read")
            .await
            .unwrap()
            .result
            .is_ok()
    );
}

#[tokio::test]
async fn alter_column_type_honors_search_path_dependencies_and_savepoints() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create schema app").await;
    conn.ok("create table app.typed_scope (id integer primary key, value integer)")
        .await;
    conn.ok("create view app.typed_view as select value from app.typed_scope")
        .await;
    conn.ok("set search_path = app, public").await;
    assert!(
        conn.ok("alter table typed_scope alter value type text")
            .await
            .result
            .is_err()
    );
    conn.ok("drop view app.typed_view").await;

    conn.ok("begin").await;
    conn.ok("savepoint before_type").await;
    conn.ok("alter table typed_scope alter value type text")
        .await;
    conn.ok("rollback to savepoint before_type").await;
    conn.ok("commit").await;
    let insert = conn.ok("insert into typed_scope values (1, 9)").await;
    if let Err(err) = insert.result {
        panic!("insert failed: {err}");
    }
    assert_eq!(
        conn.ok("select value from app.typed_scope").await.rows(),
        vec![vec![Some("9".to_string())]]
    );
}

/// A composite `PRIMARY KEY (a, b)` enforces uniqueness over the whole tuple, not
/// each column, and supports point and leading-column lookups.
#[tokio::test]
async fn composite_primary_key_uniqueness_and_lookup() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table m (tenant integer, id integer, label text, primary key (tenant, id))",
        )
        .await
        .unwrap();

    server
        .simple_query("insert into m (tenant, id, label) values (1, 1, 'a')")
        .await
        .unwrap();
    // Same tenant, different id: allowed (differs in one key column).
    server
        .simple_query("insert into m (tenant, id, label) values (1, 2, 'b')")
        .await
        .unwrap();
    // Different tenant, same id: allowed.
    server
        .simple_query("insert into m (tenant, id, label) values (2, 1, 'c')")
        .await
        .unwrap();
    // Duplicate whole tuple: rejected.
    let err = server
        .simple_query("insert into m (tenant, id, label) values (1, 1, 'dup')")
        .await
        .err()
        .expect("duplicate composite key should be rejected");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("primary key"),
        "expected unique/primary-key violation: {}",
        err.message
    );

    // Point lookup on the full key.
    let rows = server
        .simple_query("select label from m where tenant = 1 and id = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("b".to_string())]]);

    // Leading-column lookup returns every row with that tenant.
    let rows = server
        .simple_query("select id from m where tenant = 1 order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())],]
    );
}

/// A `NOT NULL` is implied for every composite primary-key column: omitting one is
/// rejected.
#[tokio::test]
async fn composite_primary_key_columns_are_not_null() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table m (a integer, b integer, primary key (a, b))")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into m (a) values (1)")
        .await
        .err()
        .expect("omitting a primary-key column should be rejected");
    assert!(
        err.message.contains("23502"),
        "expected NotNullViolation: {}",
        err.message
    );
}

/// A column-level `UNIQUE` constraint becomes a single-column unique index that
/// rejects duplicate non-NULL values but treats NULLs as distinct.
#[tokio::test]
async fn column_unique_constraint_rejects_duplicates_but_allows_distinct_nulls() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, email text unique)")
        .await
        .unwrap();

    server
        .simple_query("insert into t (id, email) values (1, 'a@b')")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into t (id, email) values (2, 'a@b')")
        .await
        .err()
        .expect("duplicate unique value should be rejected");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("unique"),
        "expected unique violation: {}",
        err.message
    );

    // Two NULL emails coexist (SQL NULLs are distinct for a unique constraint).
    server
        .simple_query("insert into t (id, email) values (3, null)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, email) values (4, null)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select count(*) from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);
}

/// A table-level `UNIQUE (a, b)` constraint enforces uniqueness over the tuple.
#[tokio::test]
async fn table_unique_constraint_over_two_columns() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table t (id integer primary key, a integer, b integer, unique (a, b))",
        )
        .await
        .unwrap();

    server
        .simple_query("insert into t (id, a, b) values (1, 1, 1)")
        .await
        .unwrap();
    // Differs in one column: allowed.
    server
        .simple_query("insert into t (id, a, b) values (2, 1, 2)")
        .await
        .unwrap();
    // Duplicate (a, b): rejected.
    let err = server
        .simple_query("insert into t (id, a, b) values (3, 1, 1)")
        .await
        .err()
        .expect("duplicate (a, b) should be rejected");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("unique"),
        "expected unique violation: {}",
        err.message
    );
}

/// A `UNIQUE` constraint's index is rebuilt on restart (replayed from its
/// `CreateIndex` WAL record) and still enforces uniqueness.
#[tokio::test]
async fn unique_constraint_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, email text unique)")
            .await
            .unwrap();
        server
            .simple_query("insert into t (id, email) values (1, 'a@b')")
            .await
            .unwrap();
        // No checkpoint: recovery must replay both the CreateTable and the
        // auto-created unique-index CreateIndex records.
    }

    let server = restart(&path).await;
    let err = server
        .simple_query("insert into t (id, email) values (2, 'a@b')")
        .await
        .err()
        .expect("unique constraint should still be enforced after restart");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("unique"),
        "expected unique violation: {}",
        err.message
    );
}

#[tokio::test]
async fn alter_table_add_primary_key_enforces_existing_table() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table accounts (aid integer not null, bid integer, balance integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into accounts (aid, bid, balance) values (1, 10, 100), (2, 10, 200)")
        .await
        .unwrap();

    server
        .simple_query("alter table accounts add primary key (aid)")
        .await
        .unwrap();

    let err = server
        .simple_query("insert into accounts (aid, bid, balance) values (1, 20, 300)")
        .await
        .err()
        .expect("duplicate primary key should be rejected after ALTER");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("primary key"),
        "expected primary key violation: {}",
        err.message
    );

    let rows = server
        .simple_query("select balance from accounts where aid = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("200".to_string())]]);
}

#[tokio::test]
async fn alter_table_add_primary_key_rejects_bad_existing_rows_without_mutating_table() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table dupes (id integer not null, v integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into dupes (id, v) values (1, 10), (1, 20)")
        .await
        .unwrap();

    let err = server
        .simple_query("alter table dupes add primary key (id)")
        .await
        .err()
        .expect("existing duplicates should reject ADD PRIMARY KEY");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("primary key"),
        "expected primary key violation: {}",
        err.message
    );
    server
        .simple_query("insert into dupes (id, v) values (1, 30)")
        .await
        .expect("failed ALTER must leave the table without a primary key");

    server
        .simple_query("create table nulls (id integer, v integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into nulls (id, v) values (null, 10)")
        .await
        .unwrap();
    let err = server
        .simple_query("alter table nulls add primary key (id)")
        .await
        .err()
        .expect("existing NULL should reject ADD PRIMARY KEY");
    assert!(
        err.message.contains("23502") || err.message.to_lowercase().contains("null"),
        "expected not-null violation: {}",
        err.message
    );
    server
        .simple_query("insert into nulls (id, v) values (null, 20)")
        .await
        .expect("failed ALTER must leave the column nullable");
}

#[tokio::test]
async fn alter_table_drop_primary_key_allows_duplicate_keys() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, v integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, v) values (1, 10)")
        .await
        .unwrap();

    server
        .simple_query("alter table t drop primary key")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, v) values (1, 20)")
        .await
        .expect("duplicate key values should be allowed after DROP PRIMARY KEY");

    let rows = server
        .simple_query("select count(*) from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);
}

#[tokio::test]
async fn alter_table_primary_key_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table t (id integer not null, v integer)")
            .await
            .unwrap();
        server
            .simple_query("insert into t (id, v) values (1, 10)")
            .await
            .unwrap();
        server
            .simple_query("alter table only t add constraint t_pkey primary key (id)")
            .await
            .unwrap();
        // No checkpoint: recovery must replay AlterTablePrimaryKey and the
        // primary-key constraint CreateIndex.
    }

    let server = restart(&path).await;
    let err = server
        .simple_query("insert into t (id, v) values (1, 20)")
        .await
        .err()
        .expect("primary key should be enforced after restart");
    assert!(
        err.message.contains("23505") || err.message.to_lowercase().contains("primary key"),
        "expected primary key violation: {}",
        err.message
    );

    server
        .simple_query("alter table t drop constraint t_pkey")
        .await
        .unwrap();
    drop(server);

    let server = restart(&path).await;
    server
        .simple_query("insert into t (id, v) values (1, 30)")
        .await
        .expect("DROP PRIMARY KEY should survive restart");
}

/// An explicit `NULL` for a `NOT NULL` column is rejected on both INSERT and
/// UPDATE.
#[tokio::test]
async fn not_null_violation_on_insert_and_update() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, name text not null)")
        .await
        .unwrap();

    let err = server
        .simple_query("insert into t (id, name) values (1, null)")
        .await
        .err()
        .expect("explicit NULL into NOT NULL column should be rejected");
    assert!(
        err.message.contains("23502"),
        "expected NotNullViolation: {}",
        err.message
    );

    server
        .simple_query("insert into t (id, name) values (1, 'ok')")
        .await
        .unwrap();
    let err = server
        .simple_query("update t set name = null where id = 1")
        .await
        .err()
        .expect("UPDATE to NULL on NOT NULL column should be rejected");
    assert!(
        err.message.contains("23502"),
        "expected NotNullViolation: {}",
        err.message
    );
}

async fn restart(path: &Path) -> TestServer {
    TestServer::start_with_data_dir(path).await.unwrap()
}

#[tokio::test]
async fn non_finite_constant_defaults_are_rejected_and_cannot_poison_the_manifest() {
    // Regression: `DEFAULT 1e400` parses to Infinity, which serializes as
    // JSON `null` in the manifest/WAL and fails to load back — one statement
    // used to make the next startup unable to open the database.
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        let mut conn = Connection::connect(&server).await.unwrap();
        for sql in [
            "create table brick (x double precision default 1e400)",
            "create table brick (x double precision default -1e400)",
        ] {
            let outcome = conn.query(sql).await.unwrap();
            let err = outcome.result.err().expect("non-finite default rejected");
            assert_eq!(
                err.code,
                common::SqlState::NumericValueOutOfRange,
                "for `{sql}`"
            );
        }
        conn.ok("create table ok_table (x double precision default 1.5)")
            .await;
        server.force_checkpoint().await.unwrap();
    }

    // The database reopens: nothing non-finite reached the durable catalog.
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let catalog = &server.app().components.catalog;
    assert!(catalog.get_table_by_name("ok_table").unwrap().is_some());
    assert!(catalog.get_table_by_name("brick").unwrap().is_none());
}
