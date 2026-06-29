mod support;

use support::{Connection, TestServer, WorkspaceGraph};

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn e2e_returning_for_insert_update_delete() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();

    // INSERT ... RETURNING projects the inserted row, including omitted-column
    // defaults (NULL) and computed expressions.
    let rows = server
        .simple_query("insert into users (id, name) values (1, 'Ada') returning id, name")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("Ada".to_string())]]
    );

    // RETURNING * over a multi-row INSERT returns one row per inserted tuple.
    let rows = server
        .simple_query("insert into users (id, name) values (2, 'Grace'), (3, 'Hopper') returning *")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("2".to_string()), Some("Grace".to_string())],
            vec![Some("3".to_string()), Some("Hopper".to_string())],
        ]
    );

    // RETURNING expression over an INSERT that omits a nullable column.
    let rows = server
        .simple_query("insert into users (id) values (4) returning id + 10 as bumped, name")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("14".to_string()), None]]);

    // UPDATE ... RETURNING projects the NEW row.
    let rows = server
        .simple_query("update users set name = 'Lovelace' where id = 1 returning id, name")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("Lovelace".to_string())]]
    );

    // DELETE ... RETURNING projects the OLD (deleted) row.
    let rows = server
        .simple_query("delete from users where id = 4 returning id, name")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("4".to_string()), None]]);

    // An UPDATE that matches no row returns an empty result set.
    let rows = server
        .simple_query("update users set name = 'X' where id = 999 returning id")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    // Final state: ids 1,2,3 remain.
    let rows = server
        .simple_query("select id from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("3".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_on_conflict_do_nothing_skips_duplicates() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada')")
        .await
        .unwrap();

    // A conflicting key with DO NOTHING is skipped, not an error; the existing row
    // is unchanged. A multi-row insert mixes a new key and a conflicting one.
    server
        .simple_query(
            "insert into users (id, name) values (1, 'Duplicate'), (2, 'Grace') \
             on conflict (id) do nothing",
        )
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, name from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("Ada".to_string())],
            vec![Some("2".to_string()), Some("Grace".to_string())],
        ]
    );

    // DO NOTHING with no target works too, and RETURNING reports only the inserted
    // (non-skipped) rows.
    let rows = server
        .simple_query(
            "insert into users (id, name) values (2, 'Skip'), (3, 'Hopper') \
             on conflict do nothing returning id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);
}

#[tokio::test]
async fn e2e_on_conflict_do_update_upserts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table kv (k integer primary key, v integer, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into kv (k, v, note) values (1, 10, 'orig')")
        .await
        .unwrap();

    // Upsert: the conflicting row is updated. `excluded` is the proposed row; a
    // bare column is the existing row. RETURNING projects the updated row.
    let rows = server
        .simple_query(
            "insert into kv (k, v, note) values (1, 5, 'new') \
             on conflict (k) do update set v = kv.v + excluded.v, note = excluded.note \
             returning k, v, note",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("1".to_string()),
            Some("15".to_string()),
            Some("new".to_string())
        ]]
    );

    // A non-conflicting upsert inserts normally.
    server
        .simple_query(
            "insert into kv (k, v, note) values (2, 20, 'two') \
             on conflict (k) do update set v = excluded.v",
        )
        .await
        .unwrap();

    // DO UPDATE with a WHERE that fails leaves the row unchanged (no insert either).
    server
        .simple_query(
            "insert into kv (k, v, note) values (1, 100, 'skip') \
             on conflict (k) do update set v = excluded.v where kv.v > 1000",
        )
        .await
        .unwrap();

    let rows = server
        .simple_query("select k, v, note from kv order by k")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("15".to_string()),
                Some("new".to_string())
            ],
            vec![
                Some("2".to_string()),
                Some("20".to_string()),
                Some("two".to_string())
            ],
        ]
    );
}

#[tokio::test]
async fn e2e_on_conflict_secondary_unique_still_errors() {
    // The arbiter is the primary key only; a conflict on a unique secondary index
    // is not arbitrated by ON CONFLICT and still raises a unique violation.
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, email text)")
        .await
        .unwrap();
    server
        .simple_query("create unique index users_email on users (email)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, email) values (1, 'a@x')")
        .await
        .unwrap();

    // New primary key (2) but a duplicate email: ON CONFLICT (id) does not cover it.
    let result = server
        .simple_query("insert into users (id, email) values (2, 'a@x') on conflict (id) do nothing")
        .await;
    let err = match result {
        Ok(_) => panic!("expected a unique violation on the secondary index"),
        Err(err) => err,
    };
    assert!(err.message.contains("C=23505") || err.message.contains("unique"));
}

#[tokio::test]
async fn e2e_returning_over_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, n text)")
        .await;

    // RETURNING over the extended query protocol: Describe yields a RowDescription
    // and Execute streams the DataRow(s).
    let rows = conn
        .extended_execute("insert into t (id, n) values (7, 'seven') returning id, n")
        .await
        .unwrap()
        .rows();
    assert_eq!(
        rows,
        vec![vec![Some("7".to_string()), Some("seven".to_string())]]
    );
}

#[tokio::test]
async fn e2e_create_insert_select_update_delete_explain() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text, active boolean)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, active) values (1, 'Ada', true)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, active) values (2, 'Grace', false)")
        .await
        .unwrap();

    let rows = server
        .simple_query("select name from users where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("Ada".to_string())]]);

    server
        .simple_query("update users set active = true where id = 2")
        .await
        .unwrap();
    server
        .simple_query("delete from users where id = 1")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, active from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string()), Some("t".to_string())]]
    );

    let explain = server
        .simple_query("explain select name from users where id = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(explain[0][0].as_ref().unwrap().contains("IndexScan"));
}

#[tokio::test]
async fn e2e_delete_then_reinsert_same_key_succeeds() {
    // MVCC DELETE stamps xmax in place (no tombstone) and retains index entries, so
    // re-inserting the deleted primary key now succeeds: the committed-deleted
    // version no longer blocks it.
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada')")
        .await
        .unwrap();
    server
        .simple_query("delete from users where id = 1")
        .await
        .unwrap();

    // The deleted row is hidden.
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    // Re-inserting the same key now succeeds (previously a tombstone-then-reinsert;
    // now a committed-delete + insert).
    server
        .simple_query("insert into users (id, name) values (1, 'Bea')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("Bea".to_string())]]
    );
}

#[tokio::test]
async fn e2e_update_new_version_is_visible_via_seq_and_index_scans() {
    // MVCC UPDATE writes a new heap version and inserts a per-version entry into
    // *every* index (the changed-column index and the unchanged-column index), so
    // after a committed UPDATE a SELECT sees the new value via a sequential scan,
    // an index scan on the changed column, AND a scan on an unchanged secondary
    // value (the anti-HOT-bug check: the unchanged-column index got an entry too).
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text, city text)")
        .await
        .unwrap();
    // city is changed by the update; name is the unchanged secondary value.
    server
        .simple_query("create index users_name on users (name)")
        .await
        .unwrap();
    server
        .simple_query("create index users_city on users (city)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, city) values (1, 'Ada', 'paris')")
        .await
        .unwrap();

    server
        .simple_query("update users set city = 'london' where id = 1")
        .await
        .unwrap();

    // Sequential scan sees the new value.
    let rows = server
        .simple_query("select id, name, city from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("1".to_string()),
            Some("Ada".to_string()),
            Some("london".to_string()),
        ]]
    );

    // Index scan on the CHANGED column (city) returns the new version; the old
    // value resolves nothing.
    let rows = server
        .simple_query("select id from users where city = 'london'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    let rows = server
        .simple_query("select id from users where city = 'paris'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    // Index scan on the UNCHANGED column (name) STILL returns the row: the new
    // version got an entry in the unchanged-column index too.
    let rows = server
        .simple_query("select id, city from users where name = 'Ada'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("london".to_string())]]
    );
}

#[tokio::test]
async fn e2e_create_index_and_unique_constraint() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada')")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (2, 'Grace')")
        .await
        .unwrap();

    // CREATE INDEX over the real wire protocol.
    server
        .simple_query("create index users_name on users (name)")
        .await
        .unwrap();

    // Queries still return the right rows after the index is built.
    let rows = server
        .simple_query("select id from users where name = 'Ada'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // A unique index rejects a duplicate value through the protocol.
    server
        .simple_query("create unique index uq_name on users (name)")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into users (id, name) values (3, 'Ada')")
        .await
        .err()
        .expect("duplicate value should violate the unique index");
    assert!(err.message.to_lowercase().contains("unique"));

    // DROP INDEX over the protocol.
    server.simple_query("drop index uq_name").await.unwrap();
    server.simple_query("drop index users_name").await.unwrap();
}

#[tokio::test]
async fn e2e_order_by_ordinal_sorts_by_output_column() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table nums (id integer primary key, label text)")
        .await
        .unwrap();
    for (id, label) in [(3, "c"), (1, "a"), (2, "b")] {
        server
            .simple_query(&format!(
                "insert into nums (id, label) values ({id}, '{label}')"
            ))
            .await
            .unwrap();
    }

    // ORDER BY 2 sorts by the second output column (id), ascending.
    let rows = server
        .simple_query("select label, id from nums order by 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("a".to_string()), Some("1".to_string())],
            vec![Some("b".to_string()), Some("2".to_string())],
            vec![Some("c".to_string()), Some("3".to_string())],
        ]
    );

    // ORDER BY 1 DESC sorts by the first output column (id), descending.
    let rows = server
        .simple_query("select id from nums order by 1 desc")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("3".to_string())],
            vec![Some("2".to_string())],
            vec![Some("1".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_hash_join_returns_matching_rows() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text)")
        .await
        .unwrap();
    for (id, name) in [(1, "Ada"), (2, "Grace")] {
        server
            .simple_query(&format!(
                "insert into users (id, name) values ({id}, '{name}')"
            ))
            .await
            .unwrap();
    }
    for (id, owner) in [(10, "Ada"), (20, "Linus")] {
        server
            .simple_query(&format!(
                "insert into accounts (id, owner) values ({id}, '{owner}')"
            ))
            .await
            .unwrap();
    }

    // Inner equi-join on name; only Ada matches.
    let rows = server
        .simple_query(
            "select users.id, accounts.id from users join accounts \
             on users.name = accounts.owner order by users.id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("10".to_string())]]
    );

    let explain = server
        .simple_query(
            "explain select users.id from users join accounts on users.name = accounts.owner",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert!(explain[0][0].as_ref().unwrap().contains("HashJoin"));
}

#[tokio::test]
async fn e2e_insert_select_from_target_table_sees_only_preexisting_rows() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    for (id, name) in [(1, "Ada"), (2, "Grace")] {
        server
            .simple_query(&format!(
                "insert into users (id, name) values ({id}, '{name}')"
            ))
            .await
            .unwrap();
    }

    // Halloween problem: reading the target table must observe only the two
    // pre-insert rows, so exactly two rows are appended (against the real
    // on-disk B-tree scan).
    server
        .simple_query("insert into users select id + 10, name from users")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("11".to_string())],
            vec![Some("12".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_scalar_functions_evaluate() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (-5, '  Ada  ')")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select upper(name), length(trim(name)), abs(id), substring(name, 3, 3) from users",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("  ADA  ".to_string()),
            Some("3".to_string()),
            Some("5".to_string()),
            Some("Ada".to_string()),
        ]]
    );
}

#[tokio::test]
async fn e2e_integer_width_aliases_behave_as_64bit_integers() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table nums (id bigint primary key, small smallint, big int8)")
        .await
        .unwrap();
    // Values beyond the 32-bit range prove all widths are backed by i64.
    server
        .simple_query(
            "insert into nums (id, small, big) values (9000000000, 5, 9223372036854775807)",
        )
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, small, big from nums")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("9000000000".to_string()),
            Some("5".to_string()),
            Some("9223372036854775807".to_string()),
        ]]
    );

    // BIGINT / INT4 are accepted CAST target types (all integer-typed).
    let rows = server
        .simple_query("select cast('9000000000' as bigint), cast(small as int4) from nums")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("9000000000".to_string()), Some("5".to_string())]]
    );
}

#[tokio::test]
async fn e2e_varchar_char_length_is_enforced() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, name varchar(5), code char(3))")
        .await
        .unwrap();

    // Values within the declared length are accepted and stored verbatim (no padding).
    server
        .simple_query("insert into t (id, name, code) values (1, 'hello', 'abc')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select name, code from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("hello".to_string()), Some("abc".to_string())]]
    );

    // VARCHAR over the limit -> 22001 (string_data_right_truncation).
    let err = server
        .simple_query("insert into t (id, name) values (2, 'toolong')")
        .await
        .err()
        .expect("over-length VARCHAR should be rejected");
    assert!(err.message.contains("22001"), "got: {}", err.message);

    // CHAR over the limit -> 22001.
    let err = server
        .simple_query("insert into t (id, code) values (3, 'abcd')")
        .await
        .err()
        .expect("over-length CHAR should be rejected");
    assert!(err.message.contains("22001"), "got: {}", err.message);

    // UPDATE that exceeds the limit is rejected too.
    let err = server
        .simple_query("update t set name = 'waytoolong' where id = 1")
        .await
        .err()
        .expect("over-length UPDATE should be rejected");
    assert!(err.message.contains("22001"), "got: {}", err.message);

    // Length is counted in characters, not bytes: 'héllo' is 5 chars (6 bytes).
    server
        .simple_query("insert into t (id, name) values (4, 'héllo')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select name from t where id = 4")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("héllo".to_string())]]);
}

#[tokio::test]
async fn e2e_date_type_round_trips_orders_and_casts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table events (id integer primary key, d date)")
        .await
        .unwrap();
    for (id, d) in [(1, "2024-02-29"), (2, "2023-01-15"), (3, "2024-12-31")] {
        server
            .simple_query(&format!(
                "insert into events (id, d) values ({id}, DATE '{d}')"
            ))
            .await
            .unwrap();
    }

    // Round-trips as YYYY-MM-DD and orders chronologically (i64-backed Ord).
    let rows = server
        .simple_query("select id, d from events order by d")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("2".to_string()), Some("2023-01-15".to_string())],
            vec![Some("1".to_string()), Some("2024-02-29".to_string())],
            vec![Some("3".to_string()), Some("2024-12-31".to_string())],
        ]
    );

    // Comparison against a date literal.
    let rows = server
        .simple_query("select id from events where d < DATE '2024-01-01'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // CAST date -> text and text -> date.
    let rows = server
        .simple_query("select cast(d as text) from events where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2024-02-29".to_string())]]);
    let rows = server
        .simple_query("select id from events where d = cast('2024-12-31' as date)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // MIN/MAX work via ordering.
    let rows = server
        .simple_query("select min(d), max(d) from events")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("2023-01-15".to_string()),
            Some("2024-12-31".to_string()),
        ]]
    );

    // An impossible date literal is rejected.
    let err = server
        .simple_query("insert into events (id, d) values (9, DATE '2023-02-29')")
        .await
        .err()
        .expect("impossible date literal should be rejected");
    assert!(
        err.message.to_lowercase().contains("date"),
        "got: {}",
        err.message
    );

    // No implicit cast: a plain string into a DATE column is a type mismatch.
    let err = server
        .simple_query("insert into events (id, d) values (9, '2024-01-01')")
        .await
        .err()
        .expect("string into date column should be rejected");
    assert!(
        err.message.contains("42804"),
        "expected datatype_mismatch, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_date_primary_key_round_trips_through_btree() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table d (day date primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into d (day, note) values (DATE '2024-01-01', 'new year')")
        .await
        .unwrap();
    // A point lookup on the DATE primary key uses the index access path, not a
    // full scan — DATE literals must produce a key candidate in the planner.
    let explain = server
        .simple_query("explain select note from d where day = DATE '2024-01-01'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "DATE primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );

    // ...and it returns the right row through the key codec.
    let rows = server
        .simple_query("select note from d where day = DATE '2024-01-01'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("new year".to_string())]]);
}

#[tokio::test]
async fn e2e_timestamp_type_round_trips_orders_and_casts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table logs (id integer primary key, at timestamp)")
        .await
        .unwrap();
    for (id, at) in [
        (1, "2024-02-29 12:30:45"),
        (2, "2023-01-15 00:00:00"),
        (3, "2024-12-31 23:59:59.5"),
    ] {
        server
            .simple_query(&format!(
                "insert into logs (id, at) values ({id}, TIMESTAMP '{at}')"
            ))
            .await
            .unwrap();
    }

    // Round-trips (fractional seconds trimmed) and orders chronologically.
    let rows = server
        .simple_query("select id, at from logs order by at")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("2".to_string()),
                Some("2023-01-15 00:00:00".to_string())
            ],
            vec![
                Some("1".to_string()),
                Some("2024-02-29 12:30:45".to_string())
            ],
            vec![
                Some("3".to_string()),
                Some("2024-12-31 23:59:59.5".to_string()),
            ],
        ]
    );

    // Comparison against a timestamp literal.
    let rows = server
        .simple_query("select id from logs where at < TIMESTAMP '2024-01-01 00:00:00'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // CAST timestamp <-> text.
    let rows = server
        .simple_query("select cast(at as text) from logs where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2024-02-29 12:30:45".to_string())]]);

    // TIMESTAMP literal without a time component defaults to midnight.
    let rows = server
        .simple_query("select id from logs where at = cast('2023-01-15' as timestamp)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // WITH TIME ZONE is unsupported.
    let err = server
        .simple_query("create table tz (id integer primary key, at timestamp with time zone)")
        .await
        .err()
        .expect("TIMESTAMP WITH TIME ZONE should be rejected");
    assert!(
        err.message.to_lowercase().contains("data type"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_timestamp_primary_key_uses_index() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (at timestamp primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (at, note) values (TIMESTAMP '2024-01-01 09:00:00', 'open')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from t where at = TIMESTAMP '2024-01-01 09:00:00'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "TIMESTAMP primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );
    let rows = server
        .simple_query("select note from t where at = TIMESTAMP '2024-01-01 09:00:00'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("open".to_string())]]);
}

#[tokio::test]
async fn e2e_bytea_type_round_trips_orders_and_casts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table blobs (id integer primary key, data bytea)")
        .await
        .unwrap();
    // \xdeadbeef, a single 0x00 byte, and the empty byte string.
    for (id, hex) in [(1, "\\xdeadbeef"), (2, "\\x00"), (3, "\\x")] {
        server
            .simple_query(&format!(
                "insert into blobs (id, data) values ({id}, BYTEA '{hex}')"
            ))
            .await
            .unwrap();
    }

    // Hex output, ordered lexicographically: "" < 0x00 < 0xdeadbeef.
    let rows = server
        .simple_query("select id, data from blobs order by data")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("3".to_string()), Some("\\x".to_string())],
            vec![Some("2".to_string()), Some("\\x00".to_string())],
            vec![Some("1".to_string()), Some("\\xdeadbeef".to_string())],
        ]
    );

    // Equality against a bytea literal.
    let rows = server
        .simple_query("select id from blobs where data = BYTEA '\\xdeadbeef'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // CAST bytea <-> text (text form is the hex string).
    let rows = server
        .simple_query("select cast(data as text) from blobs where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("\\xdeadbeef".to_string())]]);
    let rows = server
        .simple_query("select id from blobs where data = cast('\\x00' as bytea)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // Odd-length hex is rejected at parse time.
    let err = server
        .simple_query("insert into blobs (id, data) values (9, BYTEA '\\xabc')")
        .await
        .err()
        .expect("odd-length bytea literal should be rejected");
    assert!(
        err.message.to_lowercase().contains("bytea"),
        "got: {}",
        err.message
    );

    // No implicit cast: a plain string into a BYTEA column is a type mismatch.
    let err = server
        .simple_query("insert into blobs (id, data) values (9, 'hello')")
        .await
        .err()
        .expect("string into bytea column should be rejected");
    assert!(
        err.message.contains("42804"),
        "expected datatype_mismatch, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_bytea_primary_key_uses_index() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table k (h bytea primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into k (h, note) values (BYTEA '\\x0102', 'a')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from k where h = BYTEA '\\x0102'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "BYTEA primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );
    let rows = server
        .simple_query("select note from k where h = BYTEA '\\x0102'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("a".to_string())]]);
}

#[tokio::test]
async fn e2e_uuid_type_round_trips_orders_and_casts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table sessions (id integer primary key, sid uuid)")
        .await
        .unwrap();
    for (id, sid) in [
        (1, "00000000-0000-0000-0000-000000000002"),
        (2, "00000000-0000-0000-0000-000000000001"),
        (3, "ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ] {
        server
            .simple_query(&format!(
                "insert into sessions (id, sid) values ({id}, UUID '{sid}')"
            ))
            .await
            .unwrap();
    }

    // Canonical lowercase output, ordered lexicographically by the 16 bytes.
    let rows = server
        .simple_query("select id, sid from sessions order by sid")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("2".to_string()),
                Some("00000000-0000-0000-0000-000000000001".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("00000000-0000-0000-0000-000000000002".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("ffffffff-ffff-ffff-ffff-ffffffffffff".to_string()),
            ],
        ]
    );

    // Lenient input: a no-hyphen literal matches the canonical-stored value.
    let rows = server
        .simple_query("select id from sessions where sid = UUID '00000000000000000000000000000001'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // CAST uuid <-> text.
    let rows = server
        .simple_query("select cast(sid as text) from sessions where id = 3")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some(
            "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string()
        )]]
    );

    // Invalid UUID literal is rejected at parse time.
    let err = server
        .simple_query("insert into sessions (id, sid) values (9, UUID 'not-a-uuid')")
        .await
        .err()
        .expect("invalid uuid literal should be rejected");
    assert!(
        err.message.to_lowercase().contains("uuid"),
        "got: {}",
        err.message
    );

    // No implicit cast: a plain string into a UUID column is a type mismatch.
    let err = server
        .simple_query(
            "insert into sessions (id, sid) values (9, '00000000-0000-0000-0000-000000000009')",
        )
        .await
        .err()
        .expect("string into uuid column should be rejected");
    assert!(
        err.message.contains("42804"),
        "expected datatype_mismatch, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_uuid_primary_key_uses_index() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table u (id uuid primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into u (id, note) values (UUID '12345678-9abc-def0-1234-56789abcdef0', 'x')",
        )
        .await
        .unwrap();
    let explain = server
        .simple_query(
            "explain select note from u where id = UUID '12345678-9abc-def0-1234-56789abcdef0'",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "UUID primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );
    let rows = server
        .simple_query("select note from u where id = UUID '12345678-9abc-def0-1234-56789abcdef0'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("x".to_string())]]);
}

#[tokio::test]
async fn e2e_double_round_trips_arithmetic_and_aggregates() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table m (id integer primary key, x double precision)")
        .await
        .unwrap();
    for (id, x) in [(1, "2.5"), (2, "7.5"), (3, "1.0"), (4, "5.0")] {
        server
            .simple_query(&format!("insert into m (id, x) values ({id}, {x})"))
            .await
            .unwrap();
    }

    // Round-trip + ordering (1.0 prints as "1", 5.0 as "5").
    let rows = server
        .simple_query("select x from m order by x")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![Some("2.5".to_string())],
            vec![Some("5".to_string())],
            vec![Some("7.5".to_string())],
        ]
    );

    // Arithmetic: +, *, / produce doubles.
    let rows = server
        .simple_query("select x + 0.5, x * 2.0, x / 2.0 from m where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("3".to_string()),
            Some("5".to_string()),
            Some("1.25".to_string()),
        ]]
    );

    // SUM = 16, AVG = 4 (sum divisible by the row count, so both print cleanly).
    let rows = server
        .simple_query("select sum(x), avg(x), min(x), max(x) from m")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("16".to_string()),
            Some("4".to_string()),
            Some("1".to_string()),
            Some("7.5".to_string()),
        ]]
    );

    // Float division by zero errors (like PostgreSQL).
    let err = server
        .simple_query("select x / 0.0 from m where id = 1")
        .await
        .err()
        .expect("float division by zero should error");
    assert!(
        err.message.to_lowercase().contains("division by zero"),
        "got: {}",
        err.message
    );

    // Modulo is not defined for double precision.
    let err = server
        .simple_query("select x % 2.0 from m where id = 1")
        .await
        .err()
        .expect("modulo on double should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);

    // No implicit cast: an integer literal into a DOUBLE column is a mismatch.
    let err = server
        .simple_query("insert into m (id, x) values (9, 5)")
        .await
        .err()
        .expect("integer into double column should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
}

#[tokio::test]
async fn e2e_double_casts_and_special_values() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table m (id integer primary key, x double precision)")
        .await
        .unwrap();
    server
        .simple_query("insert into m (id, x) values (1, 2.5)")
        .await
        .unwrap();

    // CAST double <-> text, double <-> integer (round half-to-even).
    let rows = server
        .simple_query(
            "select cast(x as text), cast(x as integer), cast(3.5 as integer), \
             cast(5 as double precision) from m where id = 1",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("2.5".to_string()),
            Some("2".to_string()), // 2.5 -> 2 (ties to even)
            Some("4".to_string()), // 3.5 -> 4 (ties to even)
            Some("5".to_string()),
        ]]
    );

    // Special values: NaN == NaN, -0.0 == 0.0 (PostgreSQL float semantics).
    let rows = server
        .simple_query(
            "select cast('NaN' as double precision) = cast('NaN' as double precision), \
             cast('-0' as double precision) = cast('0' as double precision), \
             cast('Infinity' as double precision) > 1e308 from m where id = 1",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("t".to_string()),
            Some("t".to_string()),
            Some("t".to_string()),
        ]]
    );

    // Ordering puts -Infinity first and NaN last.
    server
        .simple_query(
            "insert into m (id, x) values \
             (2, cast('NaN' as double precision)), \
             (3, cast('-Infinity' as double precision)), \
             (4, cast('Infinity' as double precision))",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, cast(x as text) from m order by x")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("3".to_string()), Some("-Infinity".to_string())],
            vec![Some("1".to_string()), Some("2.5".to_string())],
            vec![Some("4".to_string()), Some("Infinity".to_string())],
            vec![Some("2".to_string()), Some("NaN".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_double_primary_key_uses_index() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table d (k double precision primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into d (k, note) values (3.25, 'a')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from d where k = 3.25")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "DOUBLE primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );
    let rows = server
        .simple_query("select note from d where k = 3.25")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("a".to_string())]]);
}

#[tokio::test]
async fn e2e_numeric_store_rounding_and_overflow() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, n numeric(10,2))")
        .await
        .unwrap();
    for (id, lit) in [(1, "1.239"), (2, "5"), (3, "-0.005"), (4, "99999999.99")] {
        server
            .simple_query(&format!(
                "insert into t (id, n) values ({id}, NUMERIC '{lit}')"
            ))
            .await
            .unwrap();
    }
    // Stored values are rounded to scale 2 (half away from zero) and padded.
    let rows = server
        .simple_query("select n from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1.24".to_string())],
            vec![Some("5.00".to_string())],
            vec![Some("-0.01".to_string())],
            vec![Some("99999999.99".to_string())],
        ]
    );

    // Precision overflow: NUMERIC(10,2) allows |v| < 10^8.
    let err = server
        .simple_query("insert into t (id, n) values (9, NUMERIC '100000000')")
        .await
        .err()
        .expect("numeric precision overflow should be rejected");
    assert!(
        err.message.to_lowercase().contains("overflow"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_numeric_unconstrained_scale_ordering_and_distinct() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, n numeric)")
        .await
        .unwrap();
    for (id, lit) in [(1, "1.0"), (2, "1.00"), (3, "2.5"), (4, "1.50")] {
        server
            .simple_query(&format!(
                "insert into t (id, n) values ({id}, NUMERIC '{lit}')"
            ))
            .await
            .unwrap();
    }
    // Unconstrained NUMERIC keeps each value's own display scale.
    let rows = server
        .simple_query("select n from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1.0".to_string())],
            vec![Some("1.00".to_string())],
            vec![Some("2.5".to_string())],
            vec![Some("1.50".to_string())],
        ]
    );

    // Ordering is by value: 1.0 == 1.00 (tie, broken by id), then 1.50, then 2.5.
    let rows = server
        .simple_query("select id from t order by n, id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("4".to_string())],
            vec![Some("3".to_string())],
        ]
    );

    // Equality matches by value: NUMERIC '1.0' matches both 1.0 and 1.00.
    let rows = server
        .simple_query("select count(*) from t where n = NUMERIC '1.0'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // DISTINCT collapses 1.0/1.00 into one value: {1.0, 1.5, 2.5} = 3 rows.
    let rows = server
        .simple_query("select distinct n from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows.len(), 3, "DISTINCT rows: {rows:?}");
}

#[tokio::test]
async fn e2e_numeric_casts_rejections_and_index() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, n numeric(10,2))")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, n) values (1, NUMERIC '12.34')")
        .await
        .unwrap();

    // CAST numeric<->text/integer/double (numeric->int rounds half away from zero).
    let rows = server
        .simple_query(
            "select cast(n as text), cast(n as integer), cast(NUMERIC '2.5' as integer), \
             cast(7 as numeric(10,2)), cast(n as double precision) from t where id = 1",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("12.34".to_string()),
            Some("12".to_string()),
            Some("3".to_string()), // 2.5 -> 3 (ties away)
            Some("7.00".to_string()),
            Some("12.34".to_string()),
        ]]
    );

    // No implicit casts: an integer or a double literal into a NUMERIC column.
    for bad in ["7", "3.14"] {
        let err = server
            .simple_query(&format!("insert into t (id, n) values (9, {bad})"))
            .await
            .err()
            .expect("non-numeric literal into numeric column should be rejected");
        assert!(err.message.contains("42804"), "got: {}", err.message);
    }

    // NUMERIC primary key uses an index.
    server
        .simple_query("create table k (n numeric primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into k (n, note) values (NUMERIC '3.14', 'pi')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from k where n = NUMERIC '3.14'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "NUMERIC primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );
    let rows = server
        .simple_query("select note from k where n = NUMERIC '3.14'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("pi".to_string())]]);
}

#[tokio::test]
async fn e2e_numeric_arithmetic_and_aggregates() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a numeric)")
        .await
        .unwrap();
    for (id, lit) in [(1, "1.50"), (2, "2.50")] {
        server
            .simple_query(&format!(
                "insert into t (id, a) values ({id}, NUMERIC '{lit}')"
            ))
            .await
            .unwrap();
    }

    // Arithmetic with PostgreSQL scale rules: +/- keep max scale, * sums scales.
    let rows = server
        .simple_query(
            "select cast(NUMERIC '1.50' + NUMERIC '2.00' as text), \
             cast(NUMERIC '1.50' - NUMERIC '2.00' as text), \
             cast(NUMERIC '1.50' * NUMERIC '2.00' as text) from t where id = 1",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("3.50".to_string()),
            Some("-0.50".to_string()),
            Some("3.0000".to_string()),
        ]]
    );

    // Division, modulo (defined for NUMERIC, unlike DOUBLE), and unary minus.
    let rows = server
        .simple_query(
            "select cast(NUMERIC '3' / NUMERIC '2' as text), \
             cast(NUMERIC '5.5' % NUMERIC '2' as text), \
             cast(-(NUMERIC '1.50') as text) from t where id = 1",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("1.50".to_string()), // 3 / 2 (rust_decimal division scale)
            Some("1.5".to_string()),  // 5.5 % 2
            Some("-1.50".to_string()),
        ]]
    );

    // Aggregates: SUM keeps exact scale, MIN/MAX by value, AVG is true division.
    let rows = server
        .simple_query("select cast(sum(a) as text), cast(min(a) as text), cast(max(a) as text), cast(avg(a) as integer) from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("4.00".to_string()),
            Some("1.50".to_string()),
            Some("2.50".to_string()),
            Some("2".to_string()), // avg = 2.00 -> int 2
        ]]
    );

    // Division by zero errors (like INTEGER).
    let err = server
        .simple_query("select a / NUMERIC '0' from t where id = 1")
        .await
        .err()
        .expect("numeric division by zero should error");
    assert!(
        err.message.to_lowercase().contains("division by zero"),
        "got: {}",
        err.message
    );

    // No implicit cross-type coercion: NUMERIC with INTEGER or DOUBLE.
    for bad in ["a + 1", "a + 1.0"] {
        let err = server
            .simple_query(&format!("select {bad} from t where id = 1"))
            .await
            .err()
            .expect("mixed numeric/non-numeric arithmetic should be rejected");
        assert!(
            err.message.contains("42804"),
            "for `{bad}`: {}",
            err.message
        );
    }
}

#[tokio::test]
async fn protocol_decode_error_sends_error_and_closes_connection() {
    let server = TestServer::start().await.unwrap();
    let mut stream = server.connect_raw().await.unwrap();

    stream.write_all(b"!").await.unwrap();

    let mut response = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("server did not close connection after protocol error")
        .unwrap();

    assert!(read > 0);
    assert_eq!(response[0], b'E');
}

#[test]
fn crate_dependency_graph_has_no_forbidden_edges() {
    let graph = WorkspaceGraph::load_from_manifest_dir(env!("CARGO_MANIFEST_DIR")).unwrap();

    assert!(!graph.depends_on("saguarodb-parser", "saguarodb-catalog"));
    assert!(!graph.depends_on("saguarodb-planner", "saguarodb-storage"));
    assert!(!graph.depends_on("saguarodb-storage", "saguarodb-planner"));
    assert!(!graph.any_library_depends_on("saguarodb-server"));
}

#[test]
fn dependency_graph_detects_table_style_dependency_edges() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("Cargo.toml"),
        r#"
[workspace]
members = [
  "crates/parser",
  "crates/tool",
]
"#,
    )
    .unwrap();

    let parser_dir = dir.path().join("crates/parser");
    std::fs::create_dir_all(parser_dir.join("src")).unwrap();
    std::fs::write(parser_dir.join("src/lib.rs"), "").unwrap();
    std::fs::write(
        parser_dir.join("Cargo.toml"),
        r#"
[package]
name = "saguarodb-parser"
version = "0.1.0"
edition = "2024"

[dependencies.catalog]
package = "saguarodb-catalog"
path = "../catalog"
"#,
    )
    .unwrap();

    let tool_dir = dir.path().join("crates/tool");
    std::fs::create_dir_all(tool_dir.join("src")).unwrap();
    std::fs::write(tool_dir.join("src/lib.rs"), "").unwrap();
    std::fs::write(tool_dir.join("src/main.rs"), "fn main() {}\n").unwrap();
    std::fs::write(
        tool_dir.join("Cargo.toml"),
        r#"
[package]
name = "saguarodb-tool"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "saguarodb-tool"
path = "src/main.rs"

[dependencies.server]
package = "saguarodb-server"
path = "../server"
"#,
    )
    .unwrap();

    let graph = WorkspaceGraph::load_from_manifest_dir(parser_dir.to_str().unwrap()).unwrap();

    assert!(graph.depends_on("saguarodb-parser", "saguarodb-catalog"));
    assert!(graph.any_library_depends_on("saguarodb-server"));
}

#[tokio::test]
async fn read_until_ready_times_out_when_connection_stays_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"R").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let result = support::read_until_ready_with_timeout(&mut stream, Duration::from_millis(10))
        .await
        .unwrap_err();

    assert!(
        result
            .message
            .contains("timed out waiting for ReadyForQuery")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn e2e_aggregate_distinct_deduplicates_arguments() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table sales (id integer primary key, region text, amount integer)")
        .await
        .unwrap();
    for (id, region, amount) in [
        (1, "west", "10"),
        (2, "west", "10"),
        (3, "west", "20"),
        (4, "east", "30"),
        (5, "east", "30"),
        (6, "east", "null"),
    ] {
        server
            .simple_query(&format!(
                "insert into sales (id, region, amount) values ({id}, '{region}', {amount})"
            ))
            .await
            .unwrap();
    }

    // count(distinct amount) dedups {10,20,30} and ignores the NULL => 3.
    let rows = server
        .simple_query("select count(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // sum(distinct amount) = 10 + 20 + 30 = 60.
    let rows = server
        .simple_query("select sum(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("60".to_string())]]);

    // avg(distinct amount) = 60 / 3 = 20.
    let rows = server
        .simple_query("select avg(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("20".to_string())]]);

    // min/max are unaffected by DISTINCT but must still be accepted.
    let rows = server
        .simple_query("select min(distinct amount), max(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("10".to_string()), Some("30".to_string())]]
    );

    // DISTINCT applies per group.
    let rows = server
        .simple_query(
            "select region, count(distinct amount) from sales group by region order by region",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("east".to_string()), Some("1".to_string())],
            vec![Some("west".to_string()), Some("2".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_count_distinct_wildcard_is_rejected() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table sales (id integer primary key, amount integer)")
        .await
        .unwrap();

    // COUNT(DISTINCT *) is not valid SQL; DISTINCT requires an explicit argument.
    let err = server
        .simple_query("select count(distinct *) from sales")
        .await
        .err()
        .expect("expected count(distinct *) to be rejected");
    assert!(
        err.message.contains("42601"),
        "expected a syntax error, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_select_distinct_deduplicates_rows() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, region text, tier integer)")
        .await
        .unwrap();
    for (id, region, tier) in [
        (1, "west", "1"),
        (2, "west", "1"),
        (3, "west", "2"),
        (4, "east", "1"),
        (5, "east", "1"),
    ] {
        server
            .simple_query(&format!(
                "insert into t (id, region, tier) values ({id}, '{region}', {tier})"
            ))
            .await
            .unwrap();
    }

    // DISTINCT over (region, tier) collapses the duplicate (west,1) and (east,1).
    let rows = server
        .simple_query("select distinct region, tier from t order by region, tier")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("east".to_string()), Some("1".to_string())],
            vec![Some("west".to_string()), Some("1".to_string())],
            vec![Some("west".to_string()), Some("2".to_string())],
        ]
    );

    // DISTINCT over a single column.
    let rows = server
        .simple_query("select distinct region from t order by region")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("east".to_string())],
            vec![Some("west".to_string())]
        ]
    );

    // LIMIT applies to the distinct rows, not the pre-dedup rows.
    let rows = server
        .simple_query("select distinct region from t order by region limit 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("east".to_string())]]);
}

#[tokio::test]
async fn e2e_select_distinct_on_keeps_first_row_per_key() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table orders (id integer primary key, customer text, amount integer)")
        .await
        .unwrap();
    for (id, customer, amount) in [
        (1, "ada", "10"),
        (2, "ada", "30"),
        (3, "ada", "20"),
        (4, "bob", "5"),
        (5, "bob", "15"),
    ] {
        server
            .simple_query(&format!(
                "insert into orders (id, customer, amount) values ({id}, '{customer}', {amount})"
            ))
            .await
            .unwrap();
    }

    // DISTINCT ON (customer) keeps the first row per customer in ORDER BY order,
    // which (amount DESC) is the highest amount per customer.
    let rows = server
        .simple_query(
            "select distinct on (customer) customer, amount from orders \
             order by customer, amount desc",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("ada".to_string()), Some("30".to_string())],
            vec![Some("bob".to_string()), Some("15".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_without_order_by_yields_one_row_per_key() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table orders (id integer primary key, customer text)")
        .await
        .unwrap();
    for (id, customer) in [(1, "ada"), (2, "ada"), (3, "bob")] {
        server
            .simple_query(&format!(
                "insert into orders (id, customer) values ({id}, '{customer}')"
            ))
            .await
            .unwrap();
    }

    // DISTINCT ON without ORDER BY keeps an arbitrary row per key: one per customer.
    let mut rows = server
        .simple_query("select distinct on (customer) customer from orders")
        .await
        .unwrap()
        .unwrap_rows();
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![Some("ada".to_string())], vec![Some("bob".to_string())]]
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_requires_matching_leading_order_by() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table orders (id integer primary key, customer text, amount integer)")
        .await
        .unwrap();

    // ORDER BY must lead with the DISTINCT ON expressions; ordering by amount
    // first does not match DISTINCT ON (customer) => 42P10.
    let err = server
        .simple_query("select distinct on (customer) customer, amount from orders order by amount")
        .await
        .err()
        .expect("expected DISTINCT ON not matching leading ORDER BY to be rejected");
    assert!(
        err.message.contains("42P10"),
        "expected invalid_column_reference, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_non_grouped_key_in_aggregate_query_is_rejected() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table sales (id integer primary key, customer text, region text, amount integer)",
        )
        .await
        .unwrap();
    for (id, customer, region, amount) in [
        (1, "ada", "east", "10"),
        (2, "ada", "west", "20"),
        (3, "bob", "east", "5"),
    ] {
        server
            .simple_query(&format!(
                "insert into sales (id, customer, region, amount) \
                 values ({id}, '{customer}', '{region}', {amount})"
            ))
            .await
            .unwrap();
    }

    // DISTINCT ON (id) in a GROUP BY query references a column that is neither
    // grouped nor aggregated. This must be a clean GROUP BY error, never a
    // silently wrong (row-dropping) result.
    let err = server
        .simple_query(
            "select distinct on (id) customer, region, count(*) from sales \
             group by customer, region",
        )
        .await
        .err()
        .expect("DISTINCT ON of a non-grouped column in an aggregate query must be rejected");
    assert!(
        err.message.to_lowercase().contains("group by"),
        "expected a GROUP BY error, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_grouped_key_in_aggregate_query() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer)")
        .await
        .unwrap();
    for (id, a) in [(1, "1"), (2, "1"), (3, "2")] {
        server
            .simple_query(&format!("insert into t (id, a) values ({id}, {a})"))
            .await
            .unwrap();
    }

    // DISTINCT ON a grouped column is valid: each group already has a unique a,
    // so all groups survive (a=1 -> count 2, a=2 -> count 1).
    let rows = server
        .simple_query("select distinct on (a) a, count(*) from t group by a order by a")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("2".to_string())],
            vec![Some("2".to_string()), Some("1".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_duplicate_key_is_accepted() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer, c integer)")
        .await
        .unwrap();
    for (id, a, c) in [(1, "1", "10"), (2, "1", "20"), (3, "2", "30")] {
        server
            .simple_query(&format!("insert into t (id, a, c) values ({id}, {a}, {c})"))
            .await
            .unwrap();
    }

    // DISTINCT ON (a, a) is degenerate but valid: PostgreSQL de-duplicates the
    // key list, so it is DISTINCT ON (a). ORDER BY a, c is accepted (the single
    // distinct key a leads), keeping the lowest c per a.
    let rows = server
        .simple_query("select distinct on (a, a) a, c from t order by a, c")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("10".to_string())],
            vec![Some("2".to_string()), Some("30".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_rejects_non_key_before_all_keys_even_with_duplicate() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer, b integer, c integer)")
        .await
        .unwrap();

    // ORDER BY a, a, c: the repeated `a` counts once, so the non-key `c` appears
    // before the key `b` is ordered. PostgreSQL rejects this; so must we (42P10).
    let err = server
        .simple_query("select distinct on (a, b) a, b, c from t order by a, a, c")
        .await
        .err()
        .expect("a non-key ORDER BY expr before all DISTINCT ON keys must be rejected");
    assert!(
        err.message.contains("42P10"),
        "expected invalid_column_reference, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_select_distinct_on_more_keys_than_order_by_is_accepted() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer, b integer)")
        .await
        .unwrap();
    for (id, a, b) in [(1, "1", "1"), (2, "1", "2"), (3, "2", "1"), (4, "1", "1")] {
        server
            .simple_query(&format!("insert into t (id, a, b) values ({id}, {a}, {b})"))
            .await
            .unwrap();
    }

    // DISTINCT ON (a, b) with ORDER BY a alone is accepted (matches PostgreSQL):
    // a leading ORDER BY expression that is a DISTINCT ON key is enough; ON keys
    // absent from ORDER BY are allowed. One row survives per distinct (a, b).
    let mut rows = server
        .simple_query("select distinct on (a, b) a, b from t order by a")
        .await
        .unwrap()
        .unwrap_rows();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
            vec![Some("2".to_string()), Some("1".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_select_distinct_over_grouped_aggregate() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer)")
        .await
        .unwrap();
    // Groups a=1 -> 2 rows, a=2 -> 2 rows, a=3 -> 1 row, so the per-group counts
    // are {2, 2, 1}. This exercises DISTINCT over rewritten aggregate LocalRefs:
    // the Distinct node sits above Aggregate/Sort and dedups the count outputs.
    for (id, a) in [(1, "1"), (2, "1"), (3, "2"), (4, "2"), (5, "3")] {
        server
            .simple_query(&format!("insert into t (id, a) values ({id}, {a})"))
            .await
            .unwrap();
    }

    let rows = server
        .simple_query("select distinct count(*) from t group by a order by count(*)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
}

#[tokio::test]
async fn e2e_explain_select_distinct_shows_distinct_node() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, region text)")
        .await
        .unwrap();

    let explain = server
        .simple_query("explain select distinct region from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("Distinct"),
        "EXPLAIN output missing Distinct node: {:?}",
        explain[0][0]
    );
}

#[tokio::test]
async fn e2e_select_distinct_collapses_nulls() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table n (id integer primary key, v integer)")
        .await
        .unwrap();
    for (id, v) in [(1, "null"), (2, "null"), (3, "5"), (4, "5")] {
        server
            .simple_query(&format!("insert into n (id, v) values ({id}, {v})"))
            .await
            .unwrap();
    }

    // Two NULLs are not distinct from each other: {NULL, 5}.
    let mut rows = server
        .simple_query("select distinct v from n")
        .await
        .unwrap()
        .unwrap_rows();
    rows.sort();
    assert_eq!(rows, vec![vec![None], vec![Some("5".to_string())]]);
}

#[tokio::test]
async fn e2e_select_distinct_rejects_order_by_outside_select_list() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer, b integer)")
        .await
        .unwrap();

    // For SELECT DISTINCT, ORDER BY must reference the select list. `b` is not
    // projected, so this is an invalid_column_reference (42P10).
    let err = server
        .simple_query("select distinct a from t order by b")
        .await
        .err()
        .expect("expected ORDER BY outside select list to be rejected");
    assert!(
        err.message.contains("42P10"),
        "expected invalid_column_reference, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_scalar_subquery_in_projection_and_where() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada'), (2, 'Grace')")
        .await
        .unwrap();
    server
        .simple_query("insert into accounts (id, owner) values (10, 'a'), (20, 'b')")
        .await
        .unwrap();

    // Scalar subquery in the projection: every row sees the same max(id).
    let rows = server
        .simple_query("select name, (select max(id) from accounts) from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("Ada".to_string()), Some("20".to_string())],
            vec![Some("Grace".to_string()), Some("20".to_string())],
        ]
    );

    // Scalar subquery in WHERE.
    let rows = server
        .simple_query("select id from users where id = (select min(id) from users)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // An empty subquery result is NULL.
    let rows = server
        .simple_query("select (select id from accounts where id = 999) from users where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![None]]);

    // More than one row from a scalar subquery is a cardinality violation (21000).
    let err = server
        .simple_query("select (select id from accounts) from users")
        .await
        .err()
        .expect("scalar subquery returning multiple rows should be rejected");
    assert!(err.message.contains("21000"), "got: {}", err.message);
}

#[tokio::test]
async fn e2e_in_and_not_in_subquery_null_semantics() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table vals (id integer primary key, v integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    server
        .simple_query("insert into vals (id, v) values (10, 1), (20, 3)")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id from users where id in (select v from vals) order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("3".to_string())]]
    );

    // NOT IN with no NULL keeps the non-members.
    let rows = server
        .simple_query("select id from users where id not in (select v from vals) order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // A NULL in the subquery makes NOT IN never true: no rows.
    server
        .simple_query("insert into vals (id, v) values (30, null)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users where id not in (select v from vals)")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty(), "got {rows:?}");
}

#[tokio::test]
async fn e2e_exists_and_not_exists_subquery() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    // accounts empty: EXISTS removes all rows, NOT EXISTS keeps all.
    let rows = server
        .simple_query("select id from users where exists (select 1 from accounts)")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty(), "got {rows:?}");

    let rows = server
        .simple_query("select id from users where not exists (select 1 from accounts) order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );

    // Populate accounts: EXISTS now keeps all rows.
    server
        .simple_query("insert into accounts (id, owner) values (10, 'x')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users where exists (select 1 from accounts) order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
}

#[tokio::test]
async fn e2e_derived_table_in_from() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    // Column aliasing and an outer filter over a derived table.
    let rows = server
        .simple_query(
            "select d.n from (select id, name from users) as d(i, n) where i > 1 order by i",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("b".to_string())], vec![Some("c".to_string())]]
    );

    // Aggregate over a derived table that pre-filters its rows.
    let rows = server
        .simple_query("select count(*), max(x) from (select id as x from users where id >= 2) d")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string()), Some("3".to_string())]]
    );

    // Join a base table with a derived table.
    let rows = server
        .simple_query(
            "select users.name from users \
             join (select id as x from users where id = 3) d on users.id = d.x",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("c".to_string())]]);
}

#[tokio::test]
async fn e2e_plain_and_distinct_aggregate_coexist_in_one_select() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, v integer)")
        .await
        .unwrap();
    for (id, v) in [(1, "10"), (2, "10"), (3, "20")] {
        server
            .simple_query(&format!("insert into t (id, v) values ({id}, {v})"))
            .await
            .unwrap();
    }

    // count(v) and count(distinct v) over the same argument must not collapse
    // into one aggregate: 3 non-null values, 2 distinct values.
    let rows = server
        .simple_query("select count(v), count(distinct v) from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("3".to_string()), Some("2".to_string())]]
    );
}

#[tokio::test]
async fn e2e_coalesce_nullif_and_distinct_operators() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, name) values (1, null)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, name) values (2, 'Ada')")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select coalesce(name, 'none'), nullif(id, 1), \
             name is distinct from 'Ada', name is not distinct from null \
             from t order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("none".to_string()),
                None,
                Some("t".to_string()),
                Some("t".to_string()),
            ],
            vec![
                Some("Ada".to_string()),
                Some("2".to_string()),
                Some("f".to_string()),
                Some("f".to_string()),
            ],
        ]
    );

    // No implicit cast: COALESCE arguments must share a type.
    let err = server
        .simple_query("select coalesce(name, 1) from t")
        .await
        .err()
        .expect("expected type-mismatched COALESCE to be rejected");
    assert!(
        err.message.contains("42804"),
        "expected datatype_mismatch, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_ilike_and_like_escape() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, name text)")
        .await
        .unwrap();
    for (id, name) in [(1, "Ada"), (2, "bob"), (3, "50%off")] {
        server
            .simple_query(&format!("insert into t (id, name) values ({id}, '{name}')"))
            .await
            .unwrap();
    }

    // ILIKE is case-insensitive.
    let rows = server
        .simple_query("select id from t where name ilike 'a%' order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // ESCAPE makes '!%' a literal percent sign.
    let rows = server
        .simple_query("select id from t where name like '50!%off' escape '!' order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);
}

#[tokio::test]
async fn e2e_math_functions() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table m (id integer primary key, d double precision)")
        .await
        .unwrap();
    server
        .simple_query("insert into m (id, d) values (1, 2.5)")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select floor(d), ceil(d), round(d), sqrt(9), power(2, 10), mod(7, 3), abs(-5) from m",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("2".to_string()),    // floor(2.5)
            Some("3".to_string()),    // ceil(2.5)
            Some("2".to_string()),    // round(2.5) ties to even
            Some("3".to_string()),    // sqrt(9)
            Some("1024".to_string()), // power(2, 10)
            Some("1".to_string()),    // mod(7, 3)
            Some("5".to_string()),    // abs(-5)
        ]]
    );
}

#[tokio::test]
async fn e2e_string_functions() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, name) values (1, null)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, name) values (2, 'hello world')")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select concat(name, '!'), replace(name, 'o', '0'), position('world' in name), \
             left(name, 5), right(name, 5) from t order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            // CONCAT skips the NULL name; the NULL-propagating functions return NULL.
            vec![Some("!".to_string()), None, None, None, None],
            vec![
                Some("hello world!".to_string()),
                Some("hell0 w0rld".to_string()),
                Some("7".to_string()),
                Some("hello".to_string()),
                Some("world".to_string()),
            ],
        ]
    );
}

#[tokio::test]
async fn e2e_statistical_aggregates() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table s (id integer primary key, v integer, flag boolean)")
        .await
        .unwrap();
    server
        .simple_query("insert into s (id, v, flag) values (1, 1, true)")
        .await
        .unwrap();
    server
        .simple_query("insert into s (id, v, flag) values (2, 5, false)")
        .await
        .unwrap();

    // mean = 3, squared deviations 4 + 4 = 8, n = 2: var_pop = 4, stddev_pop = 2.
    let rows = server
        .simple_query("select var_pop(v), stddev_pop(v), bool_and(flag), bool_or(flag) from s")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("4".to_string()),
            Some("2".to_string()),
            Some("f".to_string()),
            Some("t".to_string()),
        ]]
    );
}

#[tokio::test]
async fn e2e_extract_from_date_and_timestamp() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, d date, ts timestamp)")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into t (id, d, ts) values \
             (1, date '2024-03-15', timestamp '2024-03-15 13:24:35')",
        )
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, d, ts) values (2, null, null)")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select extract(year from d), extract(month from d), \
             extract(hour from ts), extract(minute from ts) from t order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("2024".to_string()),
                Some("3".to_string()),
                Some("13".to_string()),
                Some("24".to_string()),
            ],
            // A NULL source propagates to NULL.
            vec![None, None, None, None],
        ]
    );
}
