mod support;

use support::{TestServer, WorkspaceGraph};

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
