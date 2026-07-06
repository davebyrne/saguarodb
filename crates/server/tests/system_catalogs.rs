mod support;

use support::{Connection, TestServer};

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
