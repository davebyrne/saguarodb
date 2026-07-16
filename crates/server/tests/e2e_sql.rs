mod support;

use support::{Connection, TestServer, WorkspaceGraph};

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn explain_text(server: &TestServer, sql: &str) -> String {
    let rows = server.simple_query(sql).await.unwrap().unwrap_rows();
    assert_eq!(rows.len(), 1, "EXPLAIN returns exactly one row: {sql}");
    assert_eq!(
        rows[0].len(),
        1,
        "EXPLAIN returns exactly one column: {sql}"
    );
    rows[0][0].clone().expect("QUERY PLAN is non-null")
}

fn explain_node_ids(text: &str) -> Vec<usize> {
    text.lines()
        .filter_map(|line| {
            let (_, suffix) = line.split_once("[node=")?;
            let (id, _) = suffix.split_once(']')?;
            id.parse().ok()
        })
        .collect()
}

fn explain_loops(text: &str) -> Vec<u64> {
    text.split("loops=")
        .skip(1)
        .filter_map(|suffix| {
            suffix
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse()
                .ok()
        })
        .collect()
}

fn explain_line<'a>(text: &'a str, label: &str, occurrence: usize) -> &'a str {
    text.lines()
        .filter(|line| line.contains(label))
        .nth(occurrence)
        .unwrap_or_else(|| panic!("missing occurrence {occurrence} of {label}: {text}"))
}

fn assert_executed_line(line: &str, rows_and_loops: &str) {
    assert!(line.contains("actual time="), "missing timing: {line}");
    assert!(line.contains(rows_and_loops), "missing metrics: {line}");
    assert!(!line.contains("never executed"), "node did not run: {line}");
}

fn protocol_message_tags(bytes: &[u8]) -> Vec<u8> {
    let mut tags = Vec::new();
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let len = i32::from_be_bytes(bytes[offset + 1..offset + 5].try_into().unwrap()) as usize;
        assert!(len >= 4 && offset + 1 + len <= bytes.len());
        tags.push(bytes[offset]);
        offset += 1 + len;
    }
    assert_eq!(offset, bytes.len(), "response ends on a frame boundary");
    tags
}

fn assert_explain_timings_are_parseable(text: &str) {
    let mut actual_count = 0;
    for suffix in text.split("actual time=").skip(1) {
        let (startup, rest) = suffix.split_once("..").expect("startup separator");
        let (total, _) = rest.split_once(' ').expect("total terminator");
        let startup = startup.parse::<f64>().expect("parse startup milliseconds");
        let total = total.parse::<f64>().expect("parse total milliseconds");
        assert!(startup.is_finite() && startup >= 0.0);
        assert!(total.is_finite() && total >= startup);
        actual_count += 1;
    }
    assert!(actual_count > 0, "at least one executed node: {text}");
    let execution = text
        .lines()
        .find_map(|line| line.strip_prefix("Execution Time: "))
        .and_then(|value| value.strip_suffix(" ms"))
        .expect("Execution Time line")
        .parse::<f64>()
        .expect("parse execution milliseconds");
    assert!(execution.is_finite() && execution >= 0.0);
}

#[tokio::test]
async fn e2e_unnest_and_generate_series_table_functions() {
    let server = TestServer::start().await.unwrap();
    let rows = server
        .simple_query("select value from unnest(ARRAY[1, NULL, 3]) as u(value)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".into())], vec![None], vec![Some("3".into())]]
    );

    let rows = server
        .simple_query("select n from generate_series(3, 1, -1) as g(n)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("3".into())],
            vec![Some("2".into())],
            vec![Some("1".into())]
        ]
    );

    let rows = server
        .simple_query("select n from generate_series(1, 0) as g(n)")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    server
        .simple_query("create table series_inputs (id integer, values integer[])")
        .await
        .unwrap();
    server
        .simple_query("insert into series_inputs values (1, ARRAY[4, 5]), (2, ARRAY[6])")
        .await
        .unwrap();
    let rows = server
        .simple_query(
            "select id, value from series_inputs, unnest(values) as u(value) order by id, value",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("4".into())],
            vec![Some("1".into()), Some("5".into())],
            vec![Some("2".into()), Some("6".into())]
        ]
    );

    server
        .simple_query("create table empty_input (id integer)")
        .await
        .unwrap();
    for sql in [
        "select n from empty_input right join generate_series(1, 2) as g(n) on true",
        "select n from empty_input full join generate_series(1, 2) as g(n) on true",
        "select n from generate_series(1, (select 2)) as g(n)",
    ] {
        let err = server
            .simple_query(sql)
            .await
            .err()
            .expect("unsupported table-function shape should fail during binding");
        assert_eq!(err.code, common::SqlState::FeatureNotSupported);
    }

    server
        .simple_query("create table join_input (id integer)")
        .await
        .unwrap();
    let err = server
        .simple_query(
            "select * from series_inputs, join_input join unnest(series_inputs.values) u(v) on true",
        )
        .await
        .err()
        .expect("table function must not cross an explicit join boundary");
    assert_eq!(err.code, common::SqlState::FeatureNotSupported);

    server
        .simple_query(
            "create view flattened_values as \
             select value from series_inputs, unnest(series_inputs.values) as u(value)",
        )
        .await
        .unwrap();
    let err = server
        .simple_query("alter table series_inputs drop column values")
        .await
        .err()
        .expect("lateral table-function column dependency should block DROP COLUMN");
    assert_eq!(err.code, common::SqlState::DependentObjectsStillExist);
    server
        .simple_query("drop view flattened_values")
        .await
        .unwrap();
    server
        .simple_query(
            "create view nested_flattened_values as \
             select d.value from series_inputs, \
             lateral (select value from unnest(series_inputs.values) as u(value)) as d",
        )
        .await
        .unwrap();
    let err = server
        .simple_query("alter table series_inputs drop column values")
        .await
        .err()
        .expect("nested lateral correlation dependency should block DROP COLUMN");
    assert_eq!(err.code, common::SqlState::DependentObjectsStillExist);

    let err = server
        .simple_query("select * from series_inputs, unnest(array_agg(id)) as u(value)")
        .await
        .err()
        .expect("aggregate table-function argument should fail during binding");
    assert_eq!(err.code, common::SqlState::DatatypeMismatch);
}

#[tokio::test]
async fn e2e_array_agg_and_string_agg() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table aggregate_values (id integer, label text)")
        .await
        .unwrap();
    server
        .simple_query("insert into aggregate_values values (1, 'a'), (2, NULL), (NULL, 'b')")
        .await
        .unwrap();

    let rows = server
        .simple_query("select array_agg(id), string_agg(label, ',') from aggregate_values")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("{1,2,NULL}".into()), Some("a,b".into())]]
    );

    let rows = server
        .simple_query(
            "select array_agg(id), string_agg(label, ',') from aggregate_values where id > 99",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![None, None]]);

    server
        .simple_query("create table aggregate_edges (id integer, value text, delimiter text)")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into aggregate_edges values \
             (1, 'a', ','), (1, 'a', ','), (NULL, NULL, ','), (NULL, NULL, ','), \
             (2, 'a', ';'), (3, 'b', NULL)",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query(
            "select array_agg(distinct id), string_agg(distinct value, delimiter), \
             string_agg(value, delimiter) from aggregate_edges",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("{1,NULL,2,3}".into()),
            Some("a;ab".into()),
            Some("a,a;ab".into()),
        ]]
    );
    let rows = server
        .simple_query(
            "select string_agg(value, delimiter) from aggregate_edges where value is null",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![None]]);
}

#[tokio::test]
async fn e2e_array_constructors_storage_subscripts_comparisons_and_any() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table array_rows (id integer primary key, values integer[])")
        .await
        .unwrap();
    server
        .simple_query("insert into array_rows values (1, ARRAY[10, 20, NULL]), (2, ARRAY[30, 40])")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select id, values[2], 20 = ANY(values), values = ARRAY[10, 20, NULL] \
             from array_rows order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".into()),
                Some("20".into()),
                Some("t".into()),
                Some("t".into()),
            ],
            vec![
                Some("2".into()),
                Some("40".into()),
                Some("f".into()),
                Some("f".into())
            ],
        ]
    );

    let rows = server
        .simple_query(
            "select ARRAY[[1, 2], [3, 4]][2][1], 9 = ANY(ARRAY[]::integer[]), \
             (ARRAY[1, 2]::text[])[2]",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("3".into()), Some("f".into()), Some("2".into())]]
    );

    server
        .simple_query("create table array_labels (values varchar(3)[])")
        .await
        .unwrap();
    server
        .simple_query("insert into array_labels values (ARRAY['one', 'two'])")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into array_labels values (ARRAY['toolong'])")
        .await
        .err()
        .expect("oversized array element should fail");
    assert_eq!(err.code, common::SqlState::StringDataRightTruncation);

    let err = server
        .simple_query("select ARRAY[ARRAY[], ARRAY[]]::integer[]")
        .await
        .err()
        .expect("nested empty array shape should fail during binding");
    assert_eq!(err.code, common::SqlState::DatatypeMismatch);
}

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
async fn e2e_returning_sequence_functions_run_only_after_successful_dml() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create sequence returning_seq")
        .await
        .unwrap();
    server
        .simple_query("create table t (id integer primary key, email text unique)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, email) values (1, 'a@x'), (2, 'b@x')")
        .await
        .unwrap();

    let duplicate_pk = match server
        .simple_query(
            "insert into t (id, email) values (1, 'c@x') returning nextval('returning_seq')",
        )
        .await
    {
        Ok(_) => panic!("duplicate insert should fail"),
        Err(err) => err,
    };
    assert!(
        duplicate_pk.message.contains("23505") || duplicate_pk.message.contains("duplicate"),
        "{}",
        duplicate_pk.message
    );

    let rows = server
        .simple_query("select nextval('returning_seq') from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    let duplicate_unique = match server
        .simple_query("update t set email = 'b@x' where id = 1 returning nextval('returning_seq')")
        .await
    {
        Ok(_) => panic!("duplicate update should fail"),
        Err(err) => err,
    };
    assert!(
        duplicate_unique.message.contains("23505") || duplicate_unique.message.contains("unique"),
        "{}",
        duplicate_unique.message
    );

    let rows = server
        .simple_query("select nextval('returning_seq') from t where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);
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
async fn e2e_toasted_text_and_bytea_round_trip_through_dml_returning() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table docs (id integer primary key, body text, payload bytea) \
             with (toast = aggressive, toast_tuple_target = 512, \
                   toast_min_value_size = 128, toast_compression = none)",
        )
        .await
        .unwrap();

    let body_a = "toast-body-a-".repeat(180);
    let body_b = "toast-body-b-".repeat(190);
    let payload_a = format!("\\x{}", "ab".repeat(1400));
    let payload_b = format!("\\x{}", "cd".repeat(1500));

    let rows = server
        .simple_query(&format!(
            "insert into docs (id, body, payload) \
             values (1, '{body_a}', BYTEA '{payload_a}') returning id, body, payload"
        ))
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("1".to_string()),
            Some(body_a.clone()),
            Some(payload_a.clone())
        ]]
    );

    let rows = server
        .simple_query("select body, payload from docs where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some(body_a.clone()), Some(payload_a.clone())]]
    );

    let rows = server
        .simple_query(&format!(
            "update docs set body = '{body_b}', payload = BYTEA '{payload_b}' \
             where id = 1 returning body, payload"
        ))
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some(body_b.clone()), Some(payload_b.clone())]]
    );

    let rows = server
        .simple_query("delete from docs where id = 1 returning body, payload")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some(body_b), Some(payload_b)]]);

    let rows = server
        .simple_query("select id from docs")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn e2e_toasted_values_work_with_secondary_and_unique_indexes() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table docs (id integer primary key, body text, slug text) \
             with (toast = aggressive, toast_tuple_target = 512, \
                   toast_min_value_size = 128, toast_compression = none)",
        )
        .await
        .unwrap();

    let body_a = "indexed-toast-body-a-".repeat(70);
    let body_b = "indexed-toast-body-b-".repeat(70);
    let slug_a = "unique-toast-slug-a-".repeat(70);
    let slug_b = "unique-toast-slug-b-".repeat(70);

    server
        .simple_query(&format!(
            "insert into docs (id, body, slug) values (1, '{body_a}', '{slug_a}')"
        ))
        .await
        .unwrap();
    server
        .simple_query("create index docs_body on docs (body)")
        .await
        .unwrap();
    server
        .simple_query("create unique index docs_slug on docs (slug)")
        .await
        .unwrap();

    let explain = server
        .simple_query(&format!(
            "explain select id from docs where body = '{body_a}'"
        ))
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "TOASTed secondary-index lookup should plan an IndexScan, got: {:?}",
        explain[0][0]
    );
    let rows = server
        .simple_query(&format!("select id from docs where body = '{body_a}'"))
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    server
        .simple_query(&format!(
            "insert into docs (id, body, slug) values (2, '{body_b}', '{slug_b}')"
        ))
        .await
        .unwrap();
    let rows = server
        .simple_query(&format!("select id from docs where body = '{body_b}'"))
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    let err = server
        .simple_query(&format!(
            "insert into docs (id, body, slug) values (3, '{body_b}', '{slug_a}')"
        ))
        .await
        .err()
        .expect("duplicate TOASTable slug should violate the unique index");
    assert!(err.message.to_lowercase().contains("unique"));
}

#[tokio::test]
async fn e2e_toasted_returning_over_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table docs (id integer primary key, body text) \
         with (toast = aggressive, toast_tuple_target = 512, \
               toast_min_value_size = 128, toast_compression = none)")
        .await;

    let body = "extended-toast-body-".repeat(160);
    let rows = conn
        .extended_execute(&format!(
            "insert into docs (id, body) values (1, '{body}') returning id, body"
        ))
        .await
        .unwrap()
        .rows();
    assert_eq!(rows, vec![vec![Some("1".to_string()), Some(body.clone())]]);

    let rows = conn
        .extended_execute(&format!("select id from docs where body = '{body}'"))
        .await
        .unwrap()
        .rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
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
async fn e2e_table_without_primary_key_supports_dml_and_secondary_index_scan() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table events (kind text, value integer)")
        .await
        .unwrap();
    server
        .simple_query("create index events_kind on events (kind)")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into events (kind, value) values ('click', 10), ('view', 20), ('click', 30)",
        )
        .await
        .unwrap();
    server
        .simple_query("update events set value = value + 1 where kind = 'click'")
        .await
        .unwrap();
    server
        .simple_query("delete from events where value = 20")
        .await
        .unwrap();

    let explain = server
        .simple_query("explain select value from events where kind = 'click'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(explain[0][0].as_ref().unwrap().contains("IndexScan"));

    let rows = server
        .simple_query("select kind, value from events where kind = 'click' order by value")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("click".to_string()), Some("11".to_string())],
            vec![Some("click".to_string()), Some("31".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_primary_key_value_update_rekeys_row() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada'), (2, 'Bea')")
        .await
        .unwrap();
    server
        .simple_query("update users set id = 3 where id = 1")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, name from users where id = 3")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("3".to_string()), Some("Ada".to_string())]]
    );

    let rows = server
        .simple_query("select id, name from users where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    let err = match server
        .simple_query("update users set id = 2 where name = 'Ada'")
        .await
    {
        Ok(_) => panic!("expected duplicate primary key update to fail"),
        Err(err) => err,
    };
    assert_eq!(err.code, common::SqlState::UniqueViolation);
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
async fn e2e_truncate_clears_heap_and_secondary_indexes() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create index users_name on users (name)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada'), (2, 'Grace')")
        .await
        .unwrap();

    server.simple_query("truncate users").await.unwrap();

    let rows = server
        .simple_query("select id, name from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());
    let rows = server
        .simple_query("select id from users where name = 'Ada'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    server
        .simple_query("insert into users (id, name) values (1, 'Bea')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users where name = 'Bea'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn e2e_alter_table_schema_evolution_rewrites_rows_and_indexes() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text, code integer)")
        .await
        .unwrap();
    server
        .simple_query("create index users_code on users (code)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, code) values (1, 'Ada', 10), (2, 'Grace', 20)")
        .await
        .unwrap();

    server
        .simple_query("alter table users add column active boolean default true")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, active from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("t".to_string())],
            vec![Some("2".to_string()), Some("t".to_string())],
        ]
    );

    server
        .simple_query("alter table users rename column active to enabled")
        .await
        .unwrap();
    server
        .simple_query("alter table users drop column name")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, code, enabled) values (3, 30, false)")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, code, enabled from users where code = 20")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("2".to_string()),
            Some("20".to_string()),
            Some("t".to_string()),
        ]]
    );
    let rows = server
        .simple_query("select id, code, enabled from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("t".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("20".to_string()),
                Some("t".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("30".to_string()),
                Some("f".to_string()),
            ],
        ]
    );

    server
        .simple_query("create table docs (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("insert into docs (id) values (1)")
        .await
        .unwrap();
    server
        .simple_query("alter table docs add column body text default 'ready'")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, body from docs")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("ready".to_string())]]
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
async fn e2e_integer_widths_store_as_i64_but_range_check_narrow_columns() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table nums (id bigint primary key, small smallint, big int8)")
        .await
        .unwrap();
    // BIGINT/INT8 store the full 64-bit range; a value beyond 32 bits round-trips.
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

    // A SMALLINT value outside its 16-bit range is rejected (not truncated), even
    // though every integer width shares one 64-bit storage type.
    let err = server
        .simple_query("insert into nums (id, small, big) values (1, 40000, 0)")
        .await
        .err()
        .expect("smallint out of range is rejected");
    assert!(
        err.message.to_lowercase().contains("out of range"),
        "unexpected error: {}",
        err.message
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

    // WITH TIME ZONE is a distinct type (TIMESTAMPTZ); a plain TIMESTAMP value
    // is therefore NOT assignable to a `timestamp with time zone` column.
    server
        .simple_query("create table tz (id integer primary key, at timestamp with time zone)")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into tz (id, at) values (1, TIMESTAMP '2024-02-29 12:30:45')")
        .await
        .err()
        .expect("a plain TIMESTAMP into a TIMESTAMPTZ column should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
}

#[tokio::test]
async fn e2e_statement_timestamp_functions_are_stable_and_assignable_to_timestamp() {
    let server = TestServer::start().await.unwrap();

    let rows = server
        .simple_query("select cast(current_timestamp as text), cast(now() as text)")
        .await
        .unwrap()
        .unwrap_rows();
    let current_timestamp = rows[0][0].as_ref().unwrap();
    let now = rows[0][1].as_ref().unwrap();
    assert_eq!(current_timestamp, now);
    assert!(
        current_timestamp.ends_with("+00"),
        "expected timestamptz UTC text, got {current_timestamp}"
    );

    server
        .simple_query("create table logs (id integer primary key, at timestamp)")
        .await
        .unwrap();
    server
        .simple_query("insert into logs (id, at) values (1, current_timestamp), (2, now())")
        .await
        .unwrap();

    let rows = server
        .simple_query("select cast(at as text) from logs order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let at = row[0].as_ref().unwrap();
        assert!(
            !at.ends_with("+00"),
            "timestamp column should render without timezone: {at}"
        );
    }
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
async fn e2e_real_round_trips_arithmetic_aggregates_and_casts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, r real)")
        .await
        .unwrap();
    for (id, lit) in [(1, "2.5"), (2, "7.5")] {
        server
            .simple_query(&format!(
                "insert into t (id, r) values ({id}, REAL '{lit}')"
            ))
            .await
            .unwrap();
    }

    // Round-trip + ordering.
    let rows = server
        .simple_query("select r from t order by r")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2.5".to_string())], vec![Some("7.5".to_string())]]
    );

    // Arithmetic, aggregates, and casts (REAL -> DOUBLE/INTEGER, INTEGER -> REAL).
    let rows = server
        .simple_query(
            "select cast(REAL '1.5' + REAL '2.0' as text), cast(REAL '1.5' * REAL '2.0' as text), \
             cast(sum(r) as text), cast(avg(r) as text), cast(cast(2 as real) as text) from t",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("3.5".to_string()),
            Some("3".to_string()),
            Some("10".to_string()), // sum 2.5 + 7.5
            Some("5".to_string()),  // avg 5.0
            Some("2".to_string()),
        ]]
    );

    // REAL primary key uses an index.
    server
        .simple_query("create table k (r real primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into k (r, note) values (REAL '2.5', 'x')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from k where r = REAL '2.5'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "REAL primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );

    // No implicit cross-family coercion: REAL with DOUBLE, or a double literal
    // into a REAL column.
    let err = server
        .simple_query("select REAL '1.5' + 1.0 from t where id = 1")
        .await
        .err()
        .expect("real + double should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
    let err = server
        .simple_query("insert into t (id, r) values (9, 1.5)")
        .await
        .err()
        .expect("double literal into real column should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
}

#[tokio::test]
async fn e2e_time_round_trips_orders_casts_and_indexes() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, tm time)")
        .await
        .unwrap();
    for (id, lit) in [(1, "13:45:30"), (2, "08:00:00"), (3, "23:59:59.5")] {
        server
            .simple_query(&format!(
                "insert into t (id, tm) values ({id}, TIME '{lit}')"
            ))
            .await
            .unwrap();
    }

    // Round-trip + ordering (fractional seconds trimmed).
    let rows = server
        .simple_query("select tm from t order by tm")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("08:00:00".to_string())],
            vec![Some("13:45:30".to_string())],
            vec![Some("23:59:59.5".to_string())],
        ]
    );

    // Comparison + CAST to text.
    let rows = server
        .simple_query("select cast(tm as text) from t where tm < TIME '12:00:00'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("08:00:00".to_string())]]);

    // TIME primary key uses an index.
    server
        .simple_query("create table k (tm time primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into k (tm, note) values (TIME '09:30:00', 'open')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from k where tm = TIME '09:30:00'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "TIME primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );

    // No implicit cast (string into a TIME column); WITH TIME ZONE unsupported.
    let err = server
        .simple_query("insert into t (id, tm) values (9, '12:00:00')")
        .await
        .err()
        .expect("string into time column should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
    let err = server
        .simple_query("create table tz (id integer primary key, tm time with time zone)")
        .await
        .err()
        .expect("TIME WITH TIME ZONE should be rejected");
    assert!(
        err.message.to_lowercase().contains("data type"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_timestamptz_normalizes_to_utc_orders_casts_and_indexes() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table e (id integer primary key, at timestamptz)")
        .await
        .unwrap();
    // Same wall clock, different offsets -> different UTC instants.
    for (id, lit) in [
        (1, "2024-01-01 12:00:00+05"), // 07:00 UTC
        (2, "2024-01-01 12:00:00-05"), // 17:00 UTC
        (3, "2024-01-01 12:00:00"),    // 12:00 UTC (no offset)
    ] {
        server
            .simple_query(&format!(
                "insert into e (id, at) values ({id}, TIMESTAMPTZ '{lit}')"
            ))
            .await
            .unwrap();
    }

    // Ordered by UTC instant; always displayed in UTC (+00).
    let rows = server
        .simple_query("select id, cast(at as text) from e order by at")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("2024-01-01 07:00:00+00".to_string())
            ],
            vec![
                Some("3".to_string()),
                Some("2024-01-01 12:00:00+00".to_string())
            ],
            vec![
                Some("2".to_string()),
                Some("2024-01-01 17:00:00+00".to_string())
            ],
        ]
    );

    // CAST TIMESTAMPTZ <-> TIMESTAMP reinterprets the same instant (UTC wall clock).
    let rows = server
        .simple_query(
            "select cast(at as timestamp), cast(cast(TIMESTAMP '2024-06-01 09:00:00' as timestamptz) as text) from e where id = 1",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("2024-01-01 07:00:00".to_string()),
            Some("2024-06-01 09:00:00+00".to_string()),
        ]]
    );

    // The `TIMESTAMP WITH TIME ZONE` spelling and a TIMESTAMPTZ primary key.
    server
        .simple_query("create table k (at timestamp with time zone primary key, note text)")
        .await
        .unwrap();
    server
        .simple_query("insert into k (at, note) values (TIMESTAMPTZ '2024-01-01 00:00:00+00', 'a')")
        .await
        .unwrap();
    let explain = server
        .simple_query("explain select note from k where at = TIMESTAMPTZ '2024-01-01 00:00:00+00'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "TIMESTAMPTZ primary-key lookup should use an IndexScan, got: {:?}",
        explain[0][0]
    );

    // No implicit cast: a plain string into a TIMESTAMPTZ column.
    let err = server
        .simple_query("insert into e (id, at) values (9, '2024-01-01 00:00:00')")
        .await
        .err()
        .expect("string into timestamptz column should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
}

#[tokio::test]
async fn e2e_interval_round_trips_orders_by_estimate_and_casts() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table e (id integer primary key, span interval)")
        .await
        .unwrap();
    for (id, lit) in [
        (1, "1 mon"),
        (2, "30 days"),
        (3, "31 days"),
        (4, "1 day 02:30:00"),
    ] {
        server
            .simple_query(&format!(
                "insert into e (id, span) values ({id}, INTERVAL '{lit}')"
            ))
            .await
            .unwrap();
    }

    // Round-trip / PostgreSQL-style formatting.
    let rows = server
        .simple_query("select cast(span as text) from e order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1 mon".to_string())],
            vec![Some("30 days".to_string())],
            vec![Some("31 days".to_string())],
            vec![Some("1 day 02:30:00".to_string())],
        ]
    );

    // Ordering by canonical estimate: 1day02:30 (~1.1d) < 1mon == 30days < 31days.
    let rows = server
        .simple_query("select id from e order by span, id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("4".to_string())],
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("3".to_string())],
        ]
    );

    // Equality is by estimate: INTERVAL '30 days' matches both 30 days and 1 mon.
    let rows = server
        .simple_query("select count(*) from e where span = INTERVAL '30 days'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);

    // DISTINCT collapses 1 mon / 30 days into one value -> 3 distinct rows.
    let rows = server
        .simple_query("select distinct span from e")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows.len(), 3, "distinct rows: {rows:?}");

    // CAST text <-> interval round-trips.
    let rows = server
        .simple_query("select cast(cast('2 years 3 mons' as interval) as text) from e where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2 years 3 mons".to_string())]]);

    // No implicit cast: a plain string into an INTERVAL column.
    let err = server
        .simple_query("insert into e (id, span) values (9, '1 day')")
        .await
        .err()
        .expect("string into interval column should be rejected");
    assert!(err.message.contains("42804"), "got: {}", err.message);
}

#[tokio::test]
async fn e2e_interval_arithmetic_is_calendar_aware() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table d (id integer primary key, dt date, ts timestamp, \
             tt time, tz timestamptz, sp interval)",
        )
        .await
        .unwrap();
    server
        .simple_query(
            "insert into d (id, dt, ts, tt, tz, sp) values \
             (1, DATE '2024-01-31', TIMESTAMP '2024-01-31 12:00:00', TIME '23:00:00', \
              TIMESTAMPTZ '2024-01-31 12:00:00+00', INTERVAL '1 mon')",
        )
        .await
        .unwrap();

    let one = |expr: &str| {
        let server = &server;
        let sql = format!("select cast(({expr}) as text) from d where id = 1");
        async move {
            server.simple_query(&sql).await.unwrap().unwrap_rows()[0][0]
                .clone()
                .unwrap()
        }
    };

    // DATE + INTERVAL -> TIMESTAMP; month add clamps and respects leap year.
    assert_eq!(one("dt + INTERVAL '1 month'").await, "2024-02-29 00:00:00");
    // TIMESTAMP +/- INTERVAL (calendar-aware).
    assert_eq!(one("ts + INTERVAL '1 month'").await, "2024-02-29 12:00:00");
    assert_eq!(one("ts - INTERVAL '1 day'").await, "2024-01-30 12:00:00");
    // TIMESTAMPTZ + INTERVAL stays UTC.
    assert_eq!(one("tz + INTERVAL '1 day'").await, "2024-02-01 12:00:00+00");
    // TIME + INTERVAL wraps mod 24h (and ignores the day component).
    assert_eq!(one("tt + INTERVAL '1 day 2 hours'").await, "01:00:00");
    // TIME ignores months/days even when subtracting a huge (i32::MIN) month count.
    assert_eq!(one("tt - INTERVAL '-2147483648 mons'").await, "23:00:00");
    // INTERVAL +/- INTERVAL, * integer, unary minus.
    assert_eq!(one("sp + INTERVAL '15 days'").await, "1 mon 15 days");
    assert_eq!(one("sp * 3").await, "3 mons");
    assert_eq!(one("- sp").await, "-1 mons");

    // Unsupported combinations are rejected (no implicit numeric coercion).
    for bad in ["ts + 1", "sp + 1", "sp * sp"] {
        let err = server
            .simple_query(&format!("select ({bad}) from d where id = 1"))
            .await
            .err()
            .unwrap_or_else(|| panic!("expected `{bad}` to be rejected"));
        assert!(
            err.message.contains("42804"),
            "`{bad}` should be a datatype mismatch, got: {}",
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
async fn e2e_correlated_subqueries_execute() {
    // Milestone S2 (docs/specs/subqueries.md section 5): correlated
    // subqueries in WHERE, the SELECT list, and HAVING execute via the Apply
    // operator with per-outer-row semantics.
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text, amount integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada'), (2, 'Grace'), (3, 'Alan')")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into accounts (id, owner, amount) values \
             (10, 'Ada', 100), (11, 'Ada', 5), (20, 'Grace', 50), (21, 'Grace', null)",
        )
        .await
        .unwrap();

    // Correlated EXISTS / NOT EXISTS in WHERE.
    let rows = server
        .simple_query(
            "select id from users where exists \
             (select 1 from accounts where accounts.owner = users.name) order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
    let rows = server
        .simple_query(
            "select id from users where not exists \
             (select 1 from accounts where accounts.owner = users.name)",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // Correlated scalar subquery in the SELECT list; empty result is NULL.
    let rows = server
        .simple_query(
            "select name, (select max(amount) from accounts a where a.owner = users.name) \
             from users order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("Ada".to_string()), Some("100".to_string())],
            vec![Some("Grace".to_string()), Some("50".to_string())],
            vec![Some("Alan".to_string()), None],
        ]
    );

    // Correlated IN: true only where a matching amount exists.
    let rows = server
        .simple_query(
            "select id from users where 100 in \
             (select amount from accounts where accounts.owner = users.name) order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // Correlated NOT IN three-valued logic: Grace's amounts include NULL, so
    // her NOT IN is NULL (filtered); Ada's contains 100 (false); Alan's set is
    // empty (true).
    let rows = server
        .simple_query(
            "select id from users where 100 not in \
             (select amount from accounts where accounts.owner = users.name) order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // EXISTS over an implicitly-aggregated body: exactly one row always, so
    // every user qualifies (the Apply path must be taken, not a semi join).
    let rows = server
        .simple_query(
            "select id from users where exists \
             (select max(id) from accounts where accounts.owner = users.name) order by id",
        )
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

    // Nested correlation (depth 2): only Ada has an account strictly cheaper
    // than another of her own accounts.
    let rows = server
        .simple_query(
            "select id from users u where exists \
             (select 1 from accounts a where a.owner = u.name and exists \
              (select 1 from accounts a2 where a2.owner = u.name and a2.amount > a.amount))",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // Correlated HAVING against the grouped column.
    let rows = server
        .simple_query(
            "select owner, count(*) from accounts group by owner having exists \
             (select 1 from users where users.name = accounts.owner and users.id = 1) \
             order by owner",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("Ada".to_string()), Some("2".to_string())]]
    );

    // A correlated scalar subquery returning more than one row for some outer
    // row is a per-row cardinality violation (21000).
    let err = server
        .simple_query(
            "select (select amount from accounts a where a.owner = users.name) from users",
        )
        .await
        .err()
        .expect("multi-row scalar subquery should fail");
    assert!(err.message.contains("21000"), "{}", err.message);

    // EXPLAIN: an equality-correlated EXISTS decorrelates to a hash semi
    // join; a non-equality correlation keeps the Apply.
    let rows = server
        .simple_query(
            "explain select id from users where exists \
             (select 1 from accounts where accounts.owner = users.name)",
        )
        .await
        .unwrap()
        .unwrap_rows();
    let plan = rows
        .iter()
        .map(|row| row[0].clone().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("HashJoin type=Semi"), "plan was: {plan}");
    let rows = server
        .simple_query(
            "explain select id from users where exists \
             (select 1 from accounts where accounts.owner > users.name)",
        )
        .await
        .unwrap()
        .unwrap_rows();
    let plan = rows
        .iter()
        .map(|row| row[0].clone().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("Apply (Exists)"), "plan was: {plan}");

    // Correlated WHERE drives UPDATE and DELETE through the same hoisting
    // (identity passes through the Apply).
    server
        .simple_query(
            "update users set name = 'Nameless' where not exists \
             (select 1 from accounts where accounts.owner = users.name)",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select name from users where id = 3")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("Nameless".to_string())]]);
    server
        .simple_query(
            "delete from users where not exists \
             (select 1 from accounts where accounts.owner = users.name)",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select count(*) from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);
}

#[tokio::test]
async fn e2e_correlated_subquery_volatile_not_memoized() {
    // Two outer rows share the correlation key; a volatile subplan (nextval)
    // must re-execute per outer row, a stable one is memoized either way.
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, k text)")
        .await
        .unwrap();
    server
        .simple_query("create table s (id integer primary key, k text)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, k) values (1, 'a'), (2, 'a')")
        .await
        .unwrap();
    server
        .simple_query("insert into s (id, k) values (7, 'a')")
        .await
        .unwrap();
    server.simple_query("create sequence seq1").await.unwrap();

    let rows = server
        .simple_query("select (select nextval('seq1') from s where s.k = t.k) from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
        "volatile subplan must run once per outer row"
    );
}

#[tokio::test]
async fn e2e_correlated_nested_volatile_subquery_not_memoized() {
    // A nextval hidden inside an UNCORRELATED subquery inside a NESTED Apply
    // template is invisible to a plan-only probe (the body is resolved lazily
    // per outer memo miss); the volatility probe must reach bound subquery
    // bodies so the outer Apply never memoizes a sequence-advancing subplan.
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, k text)")
        .await
        .unwrap();
    server
        .simple_query("create table s (id integer primary key, k text)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id, k) values (1, 'a'), (2, 'a')")
        .await
        .unwrap();
    server
        .simple_query("insert into s (id, k) values (7, 'a')")
        .await
        .unwrap();
    server.simple_query("create sequence seq2").await.unwrap();

    // Outer scalar subquery (correlated on t.k) contains a nested correlated
    // EXISTS whose template holds an uncorrelated nextval subquery. The
    // nested correlation is a NON-equality (>=), so it cannot decorrelate to
    // a semi join and stays a nested Apply with lazy template resolution —
    // the shape the body probe exists for.
    let rows = server
        .simple_query(
            "select (select s.id from s where s.k = t.k and exists \
              (select 1 from s s2 where s2.k >= s.k and s2.id >= (select nextval('seq2')))) \
             from t order by id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    // Both outer rows share the key 'a'; without the body probe the second
    // row would reuse the memoized result and the sequence would advance only
    // once.
    assert_eq!(rows.len(), 2);
    // The sequence is global state: two outer rows must have advanced it
    // twice (a memoized subplan would advance it once), so the next value
    // is 3.
    let rows = server
        .simple_query("select nextval('seq2')")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("3".to_string())]],
        "the sequence must advance once per outer row"
    );
}

#[tokio::test]
async fn e2e_non_first_explicit_join_slots() {
    // A non-first explicit join's ON condition binds with FROM-scope slots
    // but executes against the join's own row; the lowering rebases it.
    // Before the fix this errored ("input slot out of bounds") or silently
    // matched the wrong columns.
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table ta (x integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table tb (y integer primary key, tag text)")
        .await
        .unwrap();
    server
        .simple_query("create table tc (z integer primary key, tag text)")
        .await
        .unwrap();
    server
        .simple_query("insert into ta (x) values (1), (2)")
        .await
        .unwrap();
    server
        .simple_query("insert into tb (y, tag) values (10, 'b10'), (20, 'b20')")
        .await
        .unwrap();
    server
        .simple_query("insert into tc (z, tag) values (10, 'c10'), (30, 'c30')")
        .await
        .unwrap();

    // Equality ON (hash path): only y=z=10 matches; two ta rows multiply it.
    let rows = server
        .simple_query(
            "select ta.x, tb.tag, tc.tag from ta, tb join tc on tc.z = tb.y \
             order by ta.x",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("b10".to_string()),
                Some("c10".to_string())
            ],
            vec![
                Some("2".to_string()),
                Some("b10".to_string()),
                Some("c10".to_string())
            ],
        ]
    );

    // Non-equality ON (nested-loop path) with a LEFT join variant that
    // exercises the null-pad: no tc.z is below tb.y = 10.
    let rows = server
        .simple_query(
            "select ta.x, tb.y, tc.z from ta, tb left join tc on tc.z < tb.y \
             where ta.x = 1 order by tb.y, tc.z",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("10".to_string()), None],
            vec![
                Some("1".to_string()),
                Some("20".to_string()),
                Some("10".to_string())
            ],
        ]
    );

    // Sibling-referencing LATERAL inside a non-first explicit join: the
    // correlations rebase onto the join subtree (previously guard-rejected).
    let rows = server
        .simple_query(
            "select ta.x, l.t from ta, tb join \
             lateral (select tb.tag || '!' as t) l on true \
             where ta.x = 1 order by l.t",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("b10!".to_string())],
            vec![Some("1".to_string()), Some("b20!".to_string())],
        ]
    );

    // A lateral reference CROSSING the join boundary (to ta) still gets the
    // clean rejection: the Apply's input is the join's subtree only.
    let err = server
        .simple_query("select 1 from ta, tb join lateral (select ta.x as v) l on true")
        .await
        .err()
        .expect("boundary-crossing lateral should be rejected");
    assert!(err.message.contains("0A000"), "{}", err.message);

    // An ON reference to a hidden comma sibling is an error, not a wrong
    // answer (PostgreSQL-style invalid FROM-clause reference).
    let err = server
        .simple_query("select 1 from ta, tb join tc on tc.z = ta.x")
        .await
        .err()
        .expect("ON reference to a sibling outside the join should fail");
    assert!(err.message.contains("42P01"), "{}", err.message);

    // Correlated ON references from inside a subquery body still resolve to
    // the enclosing scope (correlation), not the hidden sibling.
    let rows = server
        .simple_query(
            "select ta.x from ta where exists \
             (select 1 from tb join tc on tc.z = tb.y and tb.y = ta.x * 10) \
             order by ta.x",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn e2e_update_from_and_delete_using() {
    // docs/specs/subqueries.md section 8: the source is an inner join of the
    // target with the FROM/USING relations; a target row matched by multiple
    // source rows is modified once (first match in scan order).
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text, plan text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text, amount integer)")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into users (id, name, plan) values \
             (1, 'Ada', 'free'), (2, 'Grace', 'free'), (3, 'Alan', 'free')",
        )
        .await
        .unwrap();
    server
        .simple_query(
            "insert into accounts (id, owner, amount) values \
             (10, 'Ada', 100), (11, 'Ada', 5), (20, 'Grace', 50)",
        )
        .await
        .unwrap();

    // SET reads the FROM table; Ada matches TWO accounts but is updated once
    // (count 1 per matched user); Alan has no match and stays untouched.
    server
        .simple_query(
            "update users set plan = 'paid-' || accounts.owner \
             from accounts where accounts.owner = users.name",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, plan from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("paid-Ada".to_string())],
            vec![Some("2".to_string()), Some("paid-Grace".to_string())],
            vec![Some("3".to_string()), Some("free".to_string())],
        ]
    );

    // RETURNING sees the updated (new) target row.
    let rows = server
        .simple_query(
            "update users set plan = 'vip' from accounts \
             where accounts.owner = users.name and accounts.amount > 60 \
             returning id, plan",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("vip".to_string())]]
    );

    // Multiple FROM items join through WHERE: only Ada owns two accounts.
    server
        .simple_query(
            "update users set plan = 'dual' from accounts a, accounts b \
             where a.owner = b.owner and a.id < b.id and a.owner = users.name",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users where plan = 'dual'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // An explicit JOIN among the FROM items works: its ON condition is
    // rebased to the join's own row (only Ada owns two accounts).
    server
        .simple_query(
            "update users set plan = 'joined' \
             from accounts a join accounts b on a.owner = b.owner and a.id < b.id \
             where a.owner = users.name",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users where plan = 'joined'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // The ON clause of a FROM-item join sees only the join's operands; a
    // reference to the target is rejected with a clear error.
    let err = server
        .simple_query(
            "update users set plan = 'x' \
             from accounts a join accounts b on a.owner = users.name",
        )
        .await
        .err()
        .expect("target reference in a FROM-item ON should be rejected");
    assert!(err.message.contains("42P01"), "{}", err.message);

    // DELETE ... USING with dedupe and RETURNING of the deleted (old) row.
    let rows = server
        .simple_query(
            "delete from users using accounts \
             where accounts.owner = users.name returning id, plan",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("joined".to_string())],
            vec![Some("2".to_string()), Some("paid-Grace".to_string())],
        ]
    );
    let rows = server
        .simple_query("select id from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // Mixing a correlated subquery into the joined WHERE still works.
    server
        .simple_query("insert into users (id, name, plan) values (4, 'Ada', 'free')")
        .await
        .unwrap();
    server
        .simple_query(
            "update users set plan = 'both' from accounts \
             where accounts.owner = users.name and exists \
             (select 1 from accounts a2 where a2.owner = users.name and a2.amount > 60)",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select id from users where plan = 'both'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("4".to_string())]]);

    // A LATERAL item after another FROM item: the identity spine continues
    // below the lateral Apply (a missing arm here used to lose row identity).
    server
        .simple_query(
            "update users set plan = l.t from accounts a, \
             lateral (select a.owner || '!' as t) l where a.owner = users.name",
        )
        .await
        .unwrap();
    let rows = server
        .simple_query("select plan from users where id = 4")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("Ada!".to_string())]]);

    // A zero-column FROM item still counts as a joined source: each target is
    // updated once, not once per source row (width cannot stand in for the
    // joined flag).
    let rows = server
        .simple_query("update users set plan = 'zc' from (select from accounts) d returning id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows.len(),
        2,
        "each surviving user must be updated exactly once, got {rows:?}"
    );
}

#[tokio::test]
async fn e2e_lateral_derived_tables() {
    // docs/specs/subqueries.md section 7: LATERAL derived tables see their
    // left siblings and re-execute per outer row.
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text, amount integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada'), (2, 'Grace'), (3, 'Alan')")
        .await
        .unwrap();
    server
        .simple_query(
            "insert into accounts (id, owner, amount) values \
             (10, 'Ada', 100), (11, 'Ada', 5), (20, 'Grace', 50)",
        )
        .await
        .unwrap();

    // Comma-form LATERAL with an aggregate body (always one row).
    let rows = server
        .simple_query(
            "select u.id, l.m from users u, \
             lateral (select max(amount) as m from accounts a where a.owner = u.name) l \
             order by u.id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("100".to_string())],
            vec![Some("2".to_string()), Some("50".to_string())],
            vec![Some("3".to_string()), None],
        ]
    );

    // Top-1-per-group: ORDER BY + LIMIT inside the lateral body; inner-join
    // semantics drop Alan (no matching rows).
    let rows = server
        .simple_query(
            "select u.id, l.amount from users u, \
             lateral (select amount from accounts a where a.owner = u.name \
                      order by amount desc limit 1) l \
             order by u.id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("100".to_string())],
            vec![Some("2".to_string()), Some("50".to_string())],
        ]
    );

    // LEFT JOIN LATERAL null-pads outer rows with no matches.
    let rows = server
        .simple_query(
            "select u.id, l.amount from users u left join \
             lateral (select amount from accounts a where a.owner = u.name \
                      order by amount desc limit 1) l on true \
             order by u.id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("100".to_string())],
            vec![Some("2".to_string()), Some("50".to_string())],
            vec![Some("3".to_string()), None],
        ]
    );

    // INNER JOIN LATERAL with an ON condition over the combined row.
    let rows = server
        .simple_query(
            "select u.id, l.amount from users u join \
             lateral (select amount from accounts a where a.owner = u.name) l \
             on l.amount > 60 order by u.id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("100".to_string())]]
    );

    // A lateral body chaining through to an enclosing subquery boundary:
    // FROM-less body referencing both the sibling (a) and the outer query (u).
    let rows = server
        .simple_query(
            "select id from users u where exists \
             (select 1 from accounts a, lateral (select a.amount + u.id as x) l \
              where a.owner = u.name and l.x > 100)",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // Multiple matches multiply the outer row (Ada has two accounts).
    let rows = server
        .simple_query(
            "select u.id, l.amount from users u, \
             lateral (select amount from accounts a where a.owner = u.name) l \
             where u.id = 1 order by l.amount",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("5".to_string())],
            vec![Some("1".to_string()), Some("100".to_string())],
        ]
    );

    // Two chained-only laterals at the same level: each Apply carries its
    // own correlation list, so l2 gets u.name — not l1's u.id (a slot-space
    // mix-up here previously produced a type-confusion error).
    let rows = server
        .simple_query(
            "select id from users u where exists \
             (select 1 from lateral (select u.id as p) l1, \
              lateral (select u.name as y) l2 where l2.y = u.name) order by id",
        )
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

    // Non-LATERAL derived tables still cannot see siblings.
    let err = server
        .simple_query(
            "select u.id from users u, \
             (select amount from accounts a where a.owner = u.name) d",
        )
        .await
        .err()
        .expect("non-lateral sibling reference should fail");
    assert!(err.message.contains("42703"), "{}", err.message);

    // LATERAL under a RIGHT/FULL join is rejected.
    let err = server
        .simple_query(
            "select u.id from users u right join \
             lateral (select amount from accounts a where a.owner = u.name) l on true",
        )
        .await
        .err()
        .expect("RIGHT JOIN LATERAL should be rejected");
    assert!(err.message.contains("0A000"), "{}", err.message);
}

#[tokio::test]
async fn e2e_correlated_subqueries_unsupported_positions() {
    // Positions the hoisting pass does not cover keep the 0A000 guard
    // (docs/specs/subqueries.md section 10).
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
        .simple_query("insert into users (id, name) values (1, 'Ada')")
        .await
        .unwrap();

    for sql in [
        // ORDER BY expression.
        "select id from users order by \
         (select max(a.id) from accounts a where a.owner = users.name)",
        // Join ON condition.
        "select u.id from users u join accounts a \
         on exists (select 1 from accounts b where b.owner = u.name)",
        // UPDATE assignment.
        "update users set name = (select owner from accounts where accounts.id = users.id)",
        // RETURNING projection.
        "insert into users (id, name) values (5, 'Eve') \
         returning (select owner from accounts where accounts.id = users.id)",
        // ON CONFLICT DO UPDATE assignment.
        "insert into users (id, name) values (1, 'Ada') on conflict (id) do update \
         set name = (select owner from accounts where accounts.id = users.id)",
    ] {
        let err = server
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("correlated subquery should be rejected: {sql}"));
        assert!(
            err.message.contains("0A000") && err.message.contains("position"),
            "expected the unsupported-position guard for {sql}: {}",
            err.message
        );
    }

    // Uncorrelated subqueries keep working everywhere they did — including
    // RETURNING and ON CONFLICT DO UPDATE, which resolve via the pre-pass.
    let rows = server
        .simple_query("select id from users where exists (select 1 from users where name = 'Ada')")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    server
        .simple_query("insert into accounts (id, owner) values (10, 'Ada')")
        .await
        .unwrap();
    let rows = server
        .simple_query(
            "insert into users (id, name) values (4, 'Kay') \
             returning id, (select max(id) from accounts)",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("4".to_string()), Some("10".to_string())]]
    );
    let rows = server
        .simple_query(
            "insert into users (id, name) values (4, 'dup') on conflict (id) do update \
             set name = (select owner from accounts where id = 10) \
             returning name",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("Ada".to_string())]]);
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

/// FROM-less scalar SELECT: a query with no FROM clause evaluates its projection
/// over a single unit row. Exercises the generalized query representation end to
/// end (parse -> bind -> plan -> execute) with a Values-backed unit source.
#[tokio::test]
async fn e2e_from_less_select() {
    let server = TestServer::start().await.unwrap();

    // A literal projection yields exactly one row.
    let rows = server.simple_query("select 1").await.unwrap().unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // Arithmetic, aliases, and multiple columns all work with no FROM.
    let rows = server
        .simple_query("select 1 + 1 as n, 'hello' as greeting")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string()), Some("hello".to_string())]]
    );

    // A FROM-less WHERE filters the single unit row: false -> no rows.
    let rows = server
        .simple_query("select 1 where false")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty(), "expected no rows, got {rows:?}");

    // ... and true keeps the row.
    let rows = server
        .simple_query("select 42 where true")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("42".to_string())]]);

    // count(*) with no FROM aggregates the single unit row, yielding 1.
    let rows = server
        .simple_query("select count(*)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // An aggregate over a FROM-less WHERE that filters the unit row away still
    // emits one grouped row: count(*) over zero input rows is 0. This exercises
    // the Aggregate(Filter(Values)) lowering shape distinct from the cases above.
    let rows = server
        .simple_query("select count(*) where false")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("0".to_string())]]);

    // A scalar subquery over a real table drives a FROM-less projection.
    server
        .simple_query("create table t (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("insert into t (id) values (1), (2), (3)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select (select count(*) from t) as total")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // A bare column reference with no FROM has nothing to resolve against.
    let err = server
        .simple_query("select id")
        .await
        .err()
        .expect("column reference without FROM should fail");
    assert!(
        err.message.contains("42703"),
        "expected UndefinedColumn: {}",
        err.message
    );

    // `SELECT *` with no FROM has nothing to expand to (matches PostgreSQL);
    // it is a syntax error rather than a degenerate zero-column row.
    let err = server
        .simple_query("select *")
        .await
        .err()
        .expect("SELECT * without FROM should fail");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError for SELECT * with no FROM: {}",
        err.message
    );
}

/// Standalone `VALUES` as a query body, and `VALUES` in FROM / IN subqueries.
/// Exercises the `QueryBody::Values` variant end to end (parse -> bind -> plan ->
/// execute), reusing the existing Values plan node.
#[tokio::test]
async fn e2e_values_query() {
    let server = TestServer::start().await.unwrap();

    // Top-level single-column VALUES -> one row per list.
    let rows = server
        .simple_query("values (1), (2), (3)")
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

    // Multi-column VALUES with mixed types.
    let rows = server
        .simple_query("values (1, 'a'), (2, 'b')")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("a".to_string())],
            vec![Some("2".to_string()), Some("b".to_string())],
        ]
    );

    // A bare NULL in a column adopts that column's type; the column is nullable.
    let rows = server
        .simple_query("values (1), (null), (3)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![None],
            vec![Some("3".to_string())],
        ]
    );

    // LIMIT / OFFSET apply to a VALUES body.
    let rows = server
        .simple_query("values (1), (2), (3), (4) limit 2 offset 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string())], vec![Some("3".to_string())]]
    );

    // VALUES in FROM as a derived table, with a column alias list.
    let rows = server
        .simple_query("select x + 1 from (values (10), (20)) as t(x)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("11".to_string())], vec![Some("21".to_string())]]
    );

    // VALUES as the right side of IN.
    let rows = server
        .simple_query("select 1 where 2 in (values (1), (2), (3))")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    let rows = server
        .simple_query("select 1 where 9 in (values (1), (2), (3))")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    // Mismatched column types across rows are rejected (no implicit casts).
    let err = server
        .simple_query("values (1), ('a')")
        .await
        .err()
        .expect("VALUES with mismatched column types should fail");
    assert!(
        err.message.contains("42804"),
        "expected DatatypeMismatch: {}",
        err.message
    );

    // Rows of differing width are rejected.
    let err = server
        .simple_query("values (1, 2), (3)")
        .await
        .err()
        .expect("VALUES with unequal row widths should fail");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError: {}",
        err.message
    );

    // ORDER BY over a bare VALUES sorts the rows (by output position).
    let rows = server
        .simple_query("values (3), (1), (2) order by 1")
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

    // ORDER BY by the synthetic output-column name, DESC, plus LIMIT.
    let rows = server
        .simple_query("values (3), (1), (2) order by column1 desc limit 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("3".to_string())], vec![Some("2".to_string())]]
    );

    // A non-position/name ORDER BY expression over VALUES is rejected.
    let err = server
        .simple_query("values (1), (2) order by column1 + 1")
        .await
        .err()
        .expect("arbitrary ORDER BY expression over VALUES should be rejected");
    assert!(
        err.message.contains("0A000"),
        "expected FeatureNotSupported: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_views_execute_resolved_query_ir_after_renames() {
    let server = TestServer::start().await.unwrap();
    for sql in [
        "create table view_left (id integer primary key, value integer)",
        "create table view_right (id integer primary key, label text)",
        "insert into view_left values (1, 10), (2, 20)",
        "insert into view_right values (1, 'one'), (2, 'two')",
        "create view join_view as select l.id, r.label from view_left l join view_right r on l.id = r.id",
        "create view cte_view as with chosen as (select id, value from view_left where value > 10) select id from chosen",
        "create view set_view as select id from view_left union select id from view_right",
        "create view aggregate_view as select count(*) as n, sum(value) as total from view_left",
        "create view coalesce_view as select id, coalesce(value, 0) as resolved, coalesce(value) as identity from view_left",
        "create view subquery_view as select id, (select label from view_right r where r.id = l.id) as label from view_left l where exists (select id from view_right r where r.id = l.id)",
        "create view in_subquery_view as select id from view_left where id in (select id from view_right)",
        "create view function_view as select n from generate_series(1, 2) as g(n)",
        "create view derived_outer_view as select d.n from view_left l left join (select 1 as n) d on false",
        "create view function_outer_view as select g.n from view_left l left join generate_series(1, 1) as g(n) on false",
        "create view system_outer_view as select c.relname from view_left l left join pg_catalog.pg_class c on false",
        "create view base_view as select id, value from view_left",
        "create view inlined_view as select id from base_view where value = 20",
        // Referenced views are stored as inlined query IR. Replacing the source
        // with an incompatible output allocates new source-column identities but
        // cannot leave the already-stored inlined view with dangling references.
        "create or replace view base_view as select cast(id as text) as id, value from view_left",
        "drop view base_view",
        "alter table view_left rename column value to amount",
        "alter table view_left rename to renamed_left",
    ] {
        server
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    assert_eq!(
        server
            .simple_query("select * from join_view order by id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![
            vec![Some("1".into()), Some("one".into())],
            vec![Some("2".into()), Some("two".into())]
        ]
    );
    assert_eq!(
        server
            .simple_query("select * from cte_view")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        server
            .simple_query("select * from set_view order by id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        server
            .simple_query("select * from aggregate_view")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("2".into()), Some("30".into())]]
    );
    assert_eq!(
        server
            .simple_query("select * from coalesce_view order by id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![
            vec![Some("1".into()), Some("10".into()), Some("10".into())],
            vec![Some("2".into()), Some("20".into()), Some("20".into())]
        ]
    );
    assert_eq!(
        server
            .simple_query("select * from subquery_view order by id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![
            vec![Some("1".into()), Some("one".into())],
            vec![Some("2".into()), Some("two".into())]
        ]
    );
    assert_eq!(
        server
            .simple_query("select * from in_subquery_view order by id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        server
            .simple_query("select * from function_view order by n")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    for view in [
        "derived_outer_view",
        "function_outer_view",
        "system_outer_view",
    ] {
        assert_eq!(
            server
                .simple_query(&format!("select * from {view}"))
                .await
                .unwrap_or_else(|error| panic!("{view}: {error:?}"))
                .unwrap_rows(),
            vec![vec![None], vec![None]]
        );
    }
    assert_eq!(
        server
            .simple_query("select * from inlined_view")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("2".into())]]
    );
}

/// Set operations (`UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT`) end to end, including
/// ORDER BY over the combined result, LIMIT, VALUES arms, derived-table use, NULL
/// de-duplication, and the reconciliation/quantifier error cases.
#[tokio::test]
async fn e2e_set_operations() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table a (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table b (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("insert into a values (1), (2), (3)")
        .await
        .unwrap();
    server
        .simple_query("insert into b values (2), (3), (4)")
        .await
        .unwrap();

    let ids = |rows: Vec<Vec<Option<String>>>| -> Vec<Option<String>> {
        rows.into_iter().map(|mut r| r.remove(0)).collect()
    };
    let n = |v: &str| Some(v.to_string());

    // UNION removes duplicates; ORDER BY (by output name) sorts the combined result.
    let rows = server
        .simple_query("select id from a union select id from b order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), n("2"), n("3"), n("4")]);

    // UNION ALL keeps duplicates.
    let rows = server
        .simple_query("select id from a union all select id from b order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        ids(rows),
        vec![n("1"), n("2"), n("2"), n("3"), n("3"), n("4")]
    );

    // INTERSECT: rows in both.
    let rows = server
        .simple_query("select id from a intersect select id from b order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("2"), n("3")]);

    // EXCEPT: left rows not in right.
    let rows = server
        .simple_query("select id from a except select id from b order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1")]);

    // ORDER BY DESC + LIMIT over the combined result.
    let rows = server
        .simple_query("select id from a union select id from b order by id desc limit 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("4"), n("3")]);

    // VALUES arms, and a set operation as a derived table.
    let rows = server
        .simple_query("values (1), (2) union values (2), (3) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), n("2"), n("3")]);
    let rows = server
        .simple_query("select x from (select id from a union select id from b) as t(x) order by x")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), n("2"), n("3"), n("4")]);

    // NULL de-duplicates against NULL in set operations (NULL == NULL here); NULLs
    // sort last by default.
    let rows = server
        .simple_query("values (1), (null) union values (null), (2) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), n("2"), None]);

    // Mismatched arm types are rejected (no implicit casts).
    let err = server
        .simple_query("select id from a union select 'x'")
        .await
        .err()
        .expect("mismatched set-operation types should fail");
    assert!(
        err.message.contains("42804"),
        "expected DatatypeMismatch: {}",
        err.message
    );

    // Mismatched column counts are rejected.
    let err = server
        .simple_query("select id from a union select 1, 2")
        .await
        .err()
        .expect("mismatched set-operation column counts should fail");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError: {}",
        err.message
    );

    // INTERSECT ALL: min(count_left, count_right) copies of each row (VALUES arms
    // carry the duplicates that tables with a primary key cannot).
    let rows = server
        .simple_query("values (1), (1), (2), (3) intersect all values (1), (2), (2) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), n("2")]);

    // EXCEPT ALL: max(0, count_left - count_right) copies of each row.
    let rows = server
        .simple_query("values (1), (1), (1), (2), (3) except all values (1), (3) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), n("1"), n("2")]);

    // The distinct forms still de-duplicate over duplicate inputs.
    let rows = server
        .simple_query("values (1), (1), (2) intersect values (1), (1) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1")]);
    let rows = server
        .simple_query("values (1), (1), (2) except values (2) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1")]);
}

/// Non-recursive CTEs (`WITH`). Each CTE is inlined as a named derived table, so
/// references work anywhere a table does (FROM, joins, subqueries), CTE bodies may
/// be VALUES or set operations, later CTEs can reference earlier ones, and a CTE
/// name shadows a catalog table. The error cases (RECURSIVE, duplicate name, self
/// reference) are checked too.
#[tokio::test]
async fn e2e_common_table_expressions() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table nums (n integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("insert into nums values (1), (2), (3), (4)")
        .await
        .unwrap();

    let ns = |rows: Vec<Vec<Option<String>>>| -> Vec<Option<String>> {
        rows.into_iter().map(|mut r| r.remove(0)).collect()
    };
    let n = |v: &str| Some(v.to_string());

    // Basic CTE referenced in the body.
    let rows = server
        .simple_query("with big as (select n from nums where n >= 3) select n from big order by n")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("3"), n("4")]);

    // Column-alias list renames the CTE's output columns.
    let rows = server
        .simple_query("with t(x) as (select n from nums) select x from t order by x")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("1"), n("2"), n("3"), n("4")]);

    // A later CTE references an earlier one.
    let rows = server
        .simple_query(
            "with a as (select n from nums where n >= 2), \
                  b as (select n from a where n <= 3) \
             select n from b order by n",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("2"), n("3")]);

    // The same CTE referenced twice (a self-join over the inlined derived table).
    let rows = server
        .simple_query(
            "with t as (select n from nums where n <= 2) \
             select t1.n from t as t1 join t as t2 on t1.n = t2.n order by t1.n",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("1"), n("2")]);

    // CTE bodies may be VALUES or set operations.
    let rows = server
        .simple_query("with v(x) as (values (10), (20)) select x from v order by x")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("10"), n("20")]);
    let rows = server
        .simple_query(
            "with u as (select n from nums where n = 1 union select n from nums where n = 4) \
             select n from u order by n",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("1"), n("4")]);

    // A CTE is visible inside a nested subquery.
    let rows = server
        .simple_query("with t as (select n from nums) select (select count(*) from t) as c")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("4")]);

    // ... including a subquery inside a VALUES row (VALUES has no FROM, but a row
    // expression may still reference an enclosing CTE).
    let rows = server
        .simple_query("with t as (select n from nums) values ((select count(*) from t))")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("4")]);

    // A CTE name shadows a catalog table of the same name.
    let rows = server
        .simple_query("with nums as (select 99 as n) select n from nums")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ns(rows), vec![n("99")]);

    // WITH RECURSIVE is not supported.
    let err = server
        .simple_query("with recursive t as (select 1) select * from t")
        .await
        .err()
        .expect("WITH RECURSIVE should be rejected");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError: {}",
        err.message
    );

    // A duplicate CTE name in one WITH is rejected.
    let err = server
        .simple_query("with t as (select 1), t as (select 2) select * from t")
        .await
        .err()
        .expect("duplicate CTE name should be rejected");
    assert!(
        err.message.contains("42601"),
        "expected SyntaxError: {}",
        err.message
    );

    // A self-reference is not recursive: the name is not yet in scope.
    let err = server
        .simple_query("with t as (select n from t) select * from t")
        .await
        .err()
        .expect("self-referential (non-recursive) CTE should fail to resolve");
    assert!(
        err.message.contains("42P01"),
        "expected UndefinedTable: {}",
        err.message
    );
}

/// A bare NULL output column in one set-operation arm adopts the sibling arm's
/// type (`NULL` alone has no type in this engine, so this is what makes
/// `SELECT 1 UNION SELECT NULL` work). Non-NULL type mismatches stay strict, and a
/// column that is NULL in *both* arms remains untyped (an explicit cast is needed).
#[tokio::test]
async fn e2e_set_operation_null_column_typing() {
    let server = TestServer::start().await.unwrap();

    let ids = |rows: Vec<Vec<Option<String>>>| -> Vec<Option<String>> {
        rows.into_iter().map(|mut r| r.remove(0)).collect()
    };
    let n = |v: &str| Some(v.to_string());

    // NULL in the right arm adopts the left arm's Integer; NULL sorts last (ASC).
    let rows = server
        .simple_query("select 1 union select null order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), None]);

    // ... and symmetrically when the NULL is in the left arm.
    let rows = server
        .simple_query("select null union select 2 order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("2"), None]);

    // Works with VALUES arms too (an all-NULL VALUES column adopts the sibling's).
    let rows = server
        .simple_query("values (1) union values (null) order by 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(ids(rows), vec![n("1"), None]);

    // Multi-column: each column resolves from the arm that types it.
    let rows = server
        .simple_query("select 1, 'a' union select null, 'b' order by 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![n("1"), n("a")], vec![None, n("b")],]);

    // A non-NULL type mismatch is still rejected (strict, no implicit casts).
    let err = server
        .simple_query("select 1 union select 'x'")
        .await
        .err()
        .expect("mismatched set-operation types should still fail");
    assert!(
        err.message.contains("42804"),
        "expected DatatypeMismatch: {}",
        err.message
    );

    // A column that is NULL in BOTH arms has no type in either — still an error
    // (an explicit cast is required).
    let err = server
        .simple_query("select null union select null")
        .await
        .err()
        .expect("a column NULL in both arms should have no type");
    assert!(
        err.message.contains("42804") || err.message.contains("42601"),
        "expected a type-determination error: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_explain_analyze_profiles_core_plan_shapes() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table profile_t (id integer primary key, group_id integer)")
        .await
        .unwrap();
    let values = (1..=200)
        .map(|id| format!("({id},{id})"))
        .collect::<Vec<_>>()
        .join(",");
    server
        .simple_query(&format!("insert into profile_t values {values}"))
        .await
        .unwrap();
    server
        .simple_query("create index profile_group_idx on profile_t (group_id)")
        .await
        .unwrap();

    let index = explain_text(
        &server,
        "explain analyze select id from profile_t where group_id = 7",
    )
    .await;
    assert!(
        index.contains("IndexScan table=profile_t") && index.contains("index=2"),
        "secondary index must be selected: {index}"
    );
    assert!(index.contains("rows=1 loops=1"), "{index}");

    server.simple_query("analyze profile_t").await.unwrap();
    let seq = explain_text(&server, "explain analyze select id from profile_t").await;
    assert!(seq.contains("SeqScan table=profile_t"), "{seq}");
    assert!(seq.contains("(rows=200)"), "{seq}");
    assert!(seq.contains("rows=200 loops=1"), "{seq}");
    assert_explain_timings_are_parseable(&seq);

    server
        .simple_query("create table profile_u (id integer primary key, label text)")
        .await
        .unwrap();
    server
        .simple_query("insert into profile_u values (7, 'seven'), (17, 'seventeen')")
        .await
        .unwrap();
    let join = explain_text(
        &server,
        "explain analyze select t.id, u.label from profile_t t join profile_u u on t.id = u.id",
    )
    .await;
    assert!(join.contains("HashJoin"), "{join}");
    assert_executed_line(explain_line(&join, "HashJoin", 0), "rows=2 loops=1");
    assert_executed_line(
        explain_line(&join, "Scan table=profile_t", 0),
        "rows=200 loops=1",
    );
    assert_executed_line(
        explain_line(&join, "Scan table=profile_u", 0),
        "rows=2 loops=1",
    );
    let join_ids = explain_node_ids(&join);
    assert!(join_ids.len() >= 3, "{join}");
    assert_eq!(
        join_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        join_ids.len(),
        "every join-tree node has a distinct id: {join}"
    );

    let blocking = explain_text(
        &server,
        "explain analyze select group_id, count(*) from profile_t group by group_id order by group_id",
    )
    .await;
    assert_executed_line(explain_line(&blocking, "Aggregate", 0), "rows=200 loops=1");
    assert_executed_line(
        explain_line(&blocking, "Sort keys=1", 0),
        "rows=200 loops=1",
    );

    let system = explain_text(
        &server,
        "explain analyze select relname from pg_catalog.pg_class",
    )
    .await;
    assert!(
        system.contains("SystemScan view=pg_catalog.pg_class"),
        "{system}"
    );
    let system_scan = explain_line(&system, "SystemScan view=pg_catalog.pg_class", 0);
    assert!(system_scan.contains("actual time="), "{system_scan}");
    assert!(system_scan.contains("loops=1"), "{system_scan}");
    assert!(!system_scan.contains("never executed"), "{system_scan}");

    let never = explain_text(&server, "explain analyze select id from profile_t limit 0").await;
    assert!(never.contains("Limit count=0"), "{never}");
    assert_executed_line(explain_line(&never, "Limit count=0", 0), "rows=0 loops=1");
    assert_executed_line(
        explain_line(&never, "Scan table=profile_t", 0),
        "rows=0 loops=1",
    );

    server
        .simple_query("create table profile_empty (id integer)")
        .await
        .unwrap();
    let empty = explain_text(&server, "explain analyze select id from profile_empty").await;
    assert!(empty.contains("rows=0 loops=1"), "{empty}");
    let never_subtree = explain_text(
        &server,
        "explain analyze select id from profile_empty where exists \
         (select 1 where profile_empty.id > 0)",
    )
    .await;
    assert!(never_subtree.contains("Apply"), "{never_subtree}");
    assert!(
        never_subtree.contains("(never executed)"),
        "{never_subtree}"
    );

    let plain = explain_text(&server, "explain select id from profile_t where id < 4").await;
    let analyzed = explain_text(
        &server,
        "explain analyze select id from profile_t where id < 4",
    )
    .await;
    assert_eq!(explain_node_ids(&plain), explain_node_ids(&analyzed));
}

#[tokio::test]
async fn e2e_explain_analyze_profiles_apply_and_init_plans() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table apply_t (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into apply_t values (1, 'xx'), (2, 'xx'), (3, 'xx')")
        .await
        .unwrap();
    server
        .simple_query("create sequence apply_profile_seq")
        .await
        .unwrap();
    explain_text(&server, "explain select nextval('apply_profile_seq')").await;
    assert_eq!(
        server
            .simple_query("select nextval('apply_profile_seq')")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]],
        "plain EXPLAIN does not advance the sequence"
    );

    let volatile = explain_text(
        &server,
        "explain analyze select id from apply_t where exists \
         (select nextval('apply_profile_seq') where apply_t.id > 0)",
    )
    .await;
    assert!(volatile.contains("Apply"), "{volatile}");
    assert!(explain_loops(&volatile).contains(&3), "{volatile}");
    assert_eq!(
        server
            .simple_query("select nextval('apply_profile_seq')")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("5".to_string())]],
        "analyzed volatile Apply executes once per outer row"
    );

    let memoized = explain_text(
        &server,
        "explain analyze select id from apply_t where exists \
         (select 1 from apply_t inner_t where inner_t.id < length(apply_t.name))",
    )
    .await;
    assert!(memoized.contains("Apply"), "{memoized}");
    assert_executed_line(
        explain_line(&memoized, "Scan table=apply_t", 1),
        "rows=1 loops=1",
    );

    let init = explain_text(
        &server,
        "explain analyze select (select 1), exists (select 1), 1 in (select 1)",
    )
    .await;
    assert!(init.contains("Init Plans:"), "{init}");
    assert!(init.contains("InitPlan 1"), "{init}");
    assert!(init.contains("InitPlan 2"), "{init}");
    assert!(init.contains("InitPlan 3"), "{init}");
    let ids = explain_node_ids(&init);
    assert_eq!(
        ids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        ids.len(),
        "main and init-plan ids are globally distinct: {init}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_explain_analyze_timeout_cancellation_and_execution_error() {
    let server = TestServer::start().await.unwrap();
    let mut timed = Connection::connect(&server).await.unwrap();
    timed.ok("set statement_timeout = '1 ms'").await;
    let timeout = timed
        .query(
            "explain analyze select count(*) \
             from generate_series(1, 10000) a(n), generate_series(1, 10000) b(n)",
        )
        .await
        .unwrap();
    let err = timeout.result.err().expect("analysis must time out");
    assert!(err.message.contains("C=57014"), "{err}");
    assert_eq!(timeout.status, b'I');

    let mut canceled = Connection::connect(&server).await.unwrap();
    let (pid, secret) = canceled.backend_key();
    let query = tokio::spawn(async move {
        let outcome = canceled
            .query(
                "explain analyze select count(*) \
                 from generate_series(1, 10000) a(n), generate_series(1, 10000) b(n)",
            )
            .await;
        (canceled, outcome)
    });
    let mut observer = Connection::connect(&server).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = observer
                .ok(&format!(
                    "select state, query from pg_stat_activity where pid = {pid}"
                ))
                .await
                .rows();
            if rows.iter().any(|row| {
                row[0].as_deref() == Some("active")
                    && row[1]
                        .as_deref()
                        .is_some_and(|query| query.starts_with("explain analyze"))
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("analysis becomes active before cancellation");
    server.send_cancel(pid, secret).await.unwrap();
    let (mut canceled, outcome) = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("canceled analysis terminates promptly")
        .unwrap();
    let outcome = outcome.unwrap();
    let err = outcome.result.err().expect("analysis must be canceled");
    assert!(err.message.contains("C=57014"), "{err}");
    assert_eq!(outcome.status, b'I');
    assert!(
        canceled.ok("select 1").await.result.is_ok(),
        "connection remains usable"
    );

    let mut failing = Connection::connect(&server).await.unwrap();
    let response = failing
        .query_raw("explain analyze select (select n from generate_series(1, 2) g(n))")
        .await
        .unwrap();
    let tags = protocol_message_tags(&response);
    assert_eq!(tags.iter().filter(|tag| **tag == b'E').count(), 1);
    assert_eq!(tags.iter().filter(|tag| **tag == b'D').count(), 0);
    assert_eq!(tags.iter().filter(|tag| **tag == b'T').count(), 0);
    assert_eq!(tags.iter().filter(|tag| **tag == b'C').count(), 0);
    assert_eq!(tags.last(), Some(&b'Z'));
}
