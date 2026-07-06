mod support;

use support::{Connection, TestServer, first_row_description};

#[tokio::test]
async fn system_catalogs_support_driver_query_shapes() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table driver_items (\
         id integer primary key, \
         code text not null default 'unset', \
         amount integer default 0)")
        .await
        .rows();
    conn.ok("create unique index driver_items_code_idx on driver_items (code)")
        .await
        .rows();

    assert_eq!(
        conn.ok("select c.relname, n.nspname, a.attname \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             join pg_catalog.pg_attribute a on a.attrelid = c.oid \
             where n.nspname = 'public' and c.relname = 'driver_items' \
             order by a.attnum")
            .await
            .rows(),
        vec![
            vec![
                Some("driver_items".to_string()),
                Some("public".to_string()),
                Some("id".to_string()),
            ],
            vec![
                Some("driver_items".to_string()),
                Some("public".to_string()),
                Some("code".to_string()),
            ],
            vec![
                Some("driver_items".to_string()),
                Some("public".to_string()),
                Some("amount".to_string()),
            ],
        ]
    );

    assert_eq!(
        conn.ok(
            "select c.column_name, c.data_type, c.is_nullable, c.column_default \
             from (\
               select table_schema, table_name, column_name, data_type, \
                      is_nullable, column_default, ordinal_position \
               from information_schema.columns \
               where table_schema = 'public'\
             ) as c \
             where c.table_name in (\
               select table_name \
               from information_schema.tables \
               where table_schema = 'public' and table_type = 'BASE TABLE'\
             ) \
             and c.table_name = 'driver_items' \
             order by c.ordinal_position"
        )
        .await
        .rows(),
        vec![
            vec![
                Some("id".to_string()),
                Some("integer".to_string()),
                Some("NO".to_string()),
                None,
            ],
            vec![
                Some("code".to_string()),
                Some("text".to_string()),
                Some("NO".to_string()),
                Some("'unset'".to_string()),
            ],
            vec![
                Some("amount".to_string()),
                Some("integer".to_string()),
                Some("YES".to_string()),
                Some("0".to_string()),
            ],
        ]
    );

    assert_eq!(
        conn.ok("select relkind, count(*) \
             from pg_catalog.pg_class \
             where relname in (\
               'driver_items', \
               'driver_items_pkey', \
               'driver_items_code_idx', \
               'pg_class'\
             ) \
             group by relkind \
             order by relkind")
            .await
            .rows(),
        vec![
            vec![Some("i".to_string()), Some("2".to_string())],
            vec![Some("r".to_string()), Some("1".to_string())],
            vec![Some("v".to_string()), Some("1".to_string())],
        ]
    );

    let explain_rows = conn
        .ok("explain select relname from pg_catalog.pg_class where relkind = 'r'")
        .await
        .rows();
    assert!(
        explain_rows
            .iter()
            .flatten()
            .flatten()
            .any(|line| line.contains("SystemScan view=pg_catalog.pg_class filter=yes")),
        "EXPLAIN should expose the system scan node: {explain_rows:?}"
    );

    let simple = conn
        .ok("select relname from pg_catalog.pg_class where relname = 'driver_items'")
        .await
        .rows();
    let extended = conn
        .extended_execute("select relname from pg_catalog.pg_class where relname = 'driver_items'")
        .await
        .unwrap()
        .rows();
    assert_eq!(extended, simple);
}

#[tokio::test]
async fn system_catalogs_preserve_shadowing_and_catalog_snapshot_divergence() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table pg_class (id integer primary key)")
        .await
        .rows();
    conn.ok("insert into pg_class (id) values (42)")
        .await
        .rows();

    assert_eq!(
        conn.ok("with pg_class as (select 7 as oid) select oid from pg_class")
            .await
            .rows(),
        vec![vec![Some("7".to_string())]]
    );
    assert_eq!(
        conn.ok("select id from pg_class").await.rows(),
        vec![vec![Some("42".to_string())]]
    );
    assert_eq!(
        conn.ok("select n.nspname, c.relkind \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.relname = 'pg_class' \
             order by n.nspname")
            .await
            .rows(),
        vec![
            vec![Some("pg_catalog".to_string()), Some("v".to_string())],
            vec![Some("public".to_string()), Some("r".to_string())],
        ]
    );

    let mut reader = Connection::connect(&server).await.unwrap();
    let mut writer = Connection::connect(&server).await.unwrap();
    writer
        .ok("create table snapshot_anchor (id integer primary key)")
        .await
        .rows();
    reader
        .ok("begin isolation level repeatable read")
        .await
        .rows();
    assert_eq!(
        reader
            .ok("select count(*) from snapshot_anchor")
            .await
            .rows(),
        vec![vec![Some("0".to_string())]]
    );

    writer
        .ok("create table rr_catalog_new (id integer primary key)")
        .await
        .rows();
    writer
        .ok("insert into rr_catalog_new (id) values (1)")
        .await
        .rows();

    assert_eq!(
        reader
            .ok("select relname from pg_catalog.pg_class where relname = 'rr_catalog_new'")
            .await
            .rows(),
        vec![vec![Some("rr_catalog_new".to_string())]]
    );
    assert_eq!(
        reader
            .ok("select count(*) from rr_catalog_new")
            .await
            .rows(),
        vec![vec![Some("0".to_string())]]
    );
    reader.ok("rollback").await.rows();
}

#[tokio::test]
async fn system_catalogs_reject_unsupported_names_with_stable_sqlstates() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let err = conn
        .ok("insert into pg_catalog.pg_class values (1)")
        .await
        .result
        .err()
        .expect("system catalog write should fail");
    assert!(err.message.contains("C=0A000"), "message: {}", err.message);

    let err = conn
        .ok("select * from nosuch.pg_class")
        .await
        .result
        .err()
        .expect("unknown schema should fail");
    assert!(err.message.contains("C=3F000"), "message: {}", err.message);

    let err = conn
        .ok("select * from columns")
        .await
        .result
        .err()
        .expect("bare information_schema view should fail");
    assert!(err.message.contains("C=42P01"), "message: {}", err.message);
}

#[tokio::test]
async fn system_catalog_describe_reports_expected_wire_types() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let mut seq = Connection::extended_parse(
        "select oid, relname, reltuples, relhasindex \
         from pg_catalog.pg_class \
         where relname = 'pg_class'",
    );
    seq.extend(Connection::extended_describe_statement(""));
    seq.extend(Connection::extended_sync());
    let response = conn.extended_raw(seq).await.unwrap();
    let fields = first_row_description(&response).unwrap();

    assert_eq!(
        fields
            .iter()
            .map(|field| (
                field.name.as_str(),
                field.type_oid,
                field.type_size,
                field.type_modifier,
                field.format_code,
                field.table_oid,
                field.attr_num
            ))
            .collect::<Vec<_>>(),
        vec![
            ("oid", 20, 8, -1, 0, 0, 0),
            ("relname", 25, -1, -1, 0, 0, 0),
            ("reltuples", 700, 4, -1, 0, 0, 0),
            ("relhasindex", 16, 1, -1, 0, 0, 0),
        ]
    );
}

#[tokio::test]
async fn pg_stat_activity_reports_live_sessions() {
    let server = TestServer::start().await.unwrap();
    let mut conn_a = Connection::connect(&server).await.unwrap();
    let mut conn_b = Connection::connect(&server).await.unwrap();
    let (a_pid, _) = conn_a.backend_key();
    let (b_pid, _) = conn_b.backend_key();

    conn_a
        .ok("SET application_name = 'activity-a'")
        .await
        .rows();
    conn_a.ok("BEGIN").await.rows();

    assert_eq!(
        conn_b
            .ok(&format!(
                "select pid, datname, usename, application_name, state, query \
                 from pg_stat_activity \
                 where pid = {a_pid}"
            ))
            .await
            .rows(),
        vec![vec![
            Some(a_pid.to_string()),
            Some("saguarodb".to_string()),
            Some("saguarodb".to_string()),
            Some("activity-a".to_string()),
            Some("idle in transaction".to_string()),
            Some("BEGIN".to_string()),
        ]]
    );

    let own_sql = "select pid, state, query from pg_stat_activity where pid = pg_backend_pid()";
    assert_eq!(
        conn_b.ok(own_sql).await.rows(),
        vec![vec![
            Some(b_pid.to_string()),
            Some("active".to_string()),
            Some(own_sql.to_string()),
        ]]
    );

    conn_a.ok("ROLLBACK").await.rows();
}

#[tokio::test]
async fn pg_stat_activity_reports_extended_execute_query_text() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    let (pid, _) = conn.backend_key();

    let sql = "select pid, state, query from pg_stat_activity where pid = pg_backend_pid()";
    assert_eq!(
        conn.extended_execute(sql).await.unwrap().rows(),
        vec![vec![
            Some(pid.to_string()),
            Some("active".to_string()),
            Some(sql.to_string()),
        ]]
    );
}

#[tokio::test]
async fn pg_stat_activity_tracks_open_copy_from() {
    let server = TestServer::start().await.unwrap();
    let mut writer = Connection::connect(&server).await.unwrap();
    let mut observer = Connection::connect(&server).await.unwrap();
    let (writer_pid, _) = writer.backend_key();

    writer
        .ok("create table copy_activity (id integer primary key)")
        .await
        .rows();

    let copy_sql = "copy copy_activity (id) from stdin";
    writer.begin_copy_from(copy_sql).await.unwrap();

    let active = observer
        .ok(&format!(
            "select state, query, xact_start \
             from pg_stat_activity \
             where pid = {writer_pid}"
        ))
        .await
        .rows();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0][0], Some("active".to_string()));
    assert_eq!(active[0][1], Some(copy_sql.to_string()));
    assert!(
        active[0][2].is_some(),
        "active COPY should report xact_start"
    );

    let completion = writer.finish_copy_from(&[b"1\n"]).await.unwrap();
    assert_eq!(completion.command_tag.as_deref(), Some("COPY 1"));

    assert_eq!(
        observer
            .ok(&format!(
                "select state, xact_start \
                 from pg_stat_activity \
                 where pid = {writer_pid}"
            ))
            .await
            .rows(),
        vec![vec![Some("idle".to_string()), None]]
    );
}

#[tokio::test]
async fn pg_stat_activity_truncates_retained_query_text() {
    let server = TestServer::start().await.unwrap();
    let mut worker = Connection::connect(&server).await.unwrap();
    let mut observer = Connection::connect(&server).await.unwrap();
    let (worker_pid, _) = worker.backend_key();

    let long_sql = format!("select '{}'", "x".repeat(1500));
    worker.ok(&long_sql).await.rows();

    let rows = observer
        .ok(&format!(
            "select query \
             from pg_stat_activity \
             where pid = {worker_pid}"
        ))
        .await
        .rows();
    assert_eq!(rows.len(), 1);
    let query = rows[0][0].as_ref().expect("query should be present");
    assert_eq!(query.len(), 1024);
    assert_eq!(query, &long_sql[..1024]);
}
