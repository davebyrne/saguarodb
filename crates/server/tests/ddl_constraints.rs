mod support;

use std::path::Path;

use support::TestServer;

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

async fn restart(path: &Path) -> TestServer {
    TestServer::start_with_data_dir(path).await.unwrap()
}
