mod support;

use support::{Connection, TestServer};

#[tokio::test]
async fn create_table_foreign_keys_enforce_writes_and_generated_names() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query(
            "create table child (id integer primary key, parent_id integer references parent)",
        )
        .await
        .unwrap();
    server
        .simple_query("insert into parent values (1)")
        .await
        .unwrap();
    server
        .simple_query("insert into child values (1, 1), (2, null)")
        .await
        .unwrap();

    let err = server
        .simple_query("insert into child values (3, 99)")
        .await
        .err()
        .expect("missing parent must fail");
    assert!(err.message.contains("23503"), "{}", err.message);
    assert!(
        err.message.contains("child_parent_id_fkey"),
        "{}",
        err.message
    );
    let err = server
        .simple_query("delete from parent where id = 1")
        .await
        .err()
        .expect("referenced parent delete must fail");
    assert!(err.message.contains("23503"), "{}", err.message);
}

#[tokio::test]
async fn create_table_foreign_keys_support_composite_unique_self_and_schemas() {
    let server = TestServer::start().await.unwrap();
    server.simple_query("create schema app").await.unwrap();
    server
        .simple_query(
            "create table app.parent (id integer primary key, a integer, b text, unique (a, b))",
        )
        .await
        .unwrap();
    server
        .simple_query(
            "create table app.child (id integer primary key, a integer, b text, \
             constraint child_pair foreign key (a, b) references app.parent(a, b) \
             on update restrict on delete restrict)",
        )
        .await
        .unwrap();
    server
        .simple_query("insert into app.parent values (1, 10, 'x')")
        .await
        .unwrap();
    server
        .simple_query("insert into app.child values (1, 10, 'x')")
        .await
        .unwrap();

    server
        .simple_query(
            "create table nodes (id integer primary key, parent_id integer references nodes)",
        )
        .await
        .unwrap();
    server
        .simple_query("insert into nodes values (1, 1)")
        .await
        .unwrap();
}

#[tokio::test]
async fn create_table_foreign_key_validation_uses_exact_declared_keys_and_types() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key, code text)")
        .await
        .unwrap();
    server
        .simple_query("create unique index parent_code_idx on parent(code)")
        .await
        .unwrap();

    for (sql, state) in [
        (
            "create table bad_key (id integer primary key, code text references parent(code))",
            "42830",
        ),
        (
            "create table bad_type (id integer primary key, parent_id bigint references parent(id))",
            "42804",
        ),
        (
            "create table bad_column (id integer primary key, parent_id integer, \
             foreign key (missing) references parent(id))",
            "42703",
        ),
    ] {
        let err = server
            .simple_query(sql)
            .await
            .err()
            .expect("invalid foreign key must fail");
        assert!(err.message.contains(state), "{sql}: {}", err.message);
    }

    server
        .simple_query("create table existing (id integer primary key)")
        .await
        .unwrap();
    let err = server
        .simple_query(
            "create table if not exists existing \
             (id integer primary key, p integer references missing(id))",
        )
        .await
        .err()
        .expect("IF NOT EXISTS must still validate the FK");
    assert!(err.message.contains("42P01"), "{}", err.message);
    let err = server
        .simple_query(
            "create table if not exists existing \
             (id integer primary key, p integer, constraint existing_pkey \
              foreign key (p) references parent(id))",
        )
        .await
        .err()
        .expect("IF NOT EXISTS must validate the FK constraint namespace");
    assert!(err.message.contains("42710"), "{}", err.message);
}

#[tokio::test]
async fn create_table_foreign_key_names_share_constraint_namespace_and_suffix() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table p1 (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table p2 (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query(
            "create table child (id integer primary key, p integer, \
             foreign key (p) references p1, foreign key (p) references p2)",
        )
        .await
        .unwrap();
    server
        .simple_query("insert into p1 values (1)")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into child values (1, 1)")
        .await
        .err()
        .expect("second generated FK must fail");
    assert!(err.message.contains("child_p_fkey1"), "{}", err.message);

    let err = server
        .simple_query(
            "create table duplicate_name (id integer primary key, p integer, \
             constraint duplicate_name_pkey foreign key (p) references p1)",
        )
        .await
        .err()
        .expect("FK name must not collide with the PK constraint");
    assert!(err.message.contains("42710"), "{}", err.message);
}

#[tokio::test]
async fn create_table_foreign_keys_survive_wal_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table parent (id integer primary key)")
            .await
            .unwrap();
        server
            .simple_query(
                "create table child (id integer primary key, parent_id integer references parent)",
            )
            .await
            .unwrap();
    }

    let server = TestServer::start_with_data_dir(&path).await.unwrap();
    let err = server
        .simple_query("insert into child values (1, 42)")
        .await
        .err()
        .expect("recovered foreign key must be enforced");
    assert!(err.message.contains("23503"), "{}", err.message);
}

#[tokio::test]
async fn create_table_foreign_keys_work_in_transactions_and_savepoints() {
    let server = TestServer::start().await.unwrap();
    let mut connection = Connection::connect(&server).await.unwrap();
    connection.query("begin").await.unwrap().unwrap();
    connection
        .query("create table parent (id integer primary key)")
        .await
        .unwrap()
        .unwrap();
    connection
        .query("savepoint before_child")
        .await
        .unwrap()
        .unwrap();
    connection
        .query("create table discarded (id integer primary key, p integer references parent)")
        .await
        .unwrap()
        .unwrap();
    connection
        .query("rollback to savepoint before_child")
        .await
        .unwrap()
        .unwrap();
    connection
        .query("create table child (id integer primary key, p integer references parent)")
        .await
        .unwrap()
        .unwrap();
    connection
        .query("insert into parent values (1)")
        .await
        .unwrap()
        .unwrap();
    connection
        .query("insert into child values (1, 1)")
        .await
        .unwrap()
        .unwrap();
    connection.query("commit").await.unwrap().unwrap();

    let err = server
        .simple_query("insert into child values (2, 99)")
        .await
        .err()
        .expect("committed transactional FK must be enforced");
    assert!(err.message.contains("23503"), "{}", err.message);
}
