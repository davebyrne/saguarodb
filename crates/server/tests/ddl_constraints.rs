mod support;

use std::path::Path;

use support::{Connection, TestServer};

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
        err.message.contains("42P01"),
        "expected UndefinedTable: {}",
        err.message
    );
}

#[tokio::test]
async fn sequence_ddl_inside_transaction_is_rejected() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("begin").await;
    let create = conn.ok("create sequence users_id_seq").await;
    let err = create
        .result
        .err()
        .expect("sequence DDL inside a transaction should fail");
    assert!(
        err.message.to_lowercase().contains("ddl"),
        "message was: {}",
        err.message
    );
    assert_eq!(create.status, b'E');
    conn.ok("rollback").await;
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
