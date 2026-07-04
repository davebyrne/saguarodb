mod support;

use support::TestServer;

/// A dictionary created AFTER the last checkpoint must be re-resolvable at
/// recovery purely from the dict file + WAL (`compression.md` §7): create a
/// zstd table, load data, checkpoint, ALTER to train the dict (post-
/// checkpoint), insert more (dict-compressed FPIs), crash-restart, verify.
#[tokio::test]
async fn dictionary_and_compressed_wal_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query(
                "create table logs (id integer primary key, body text) with (compression = 'zstd')",
            )
            .await
            .unwrap();
        // 512 rows × ~340-byte bodies ≈ 20+ heap pages: comfortably past the
        // ≥8-sample training gate (Task 2) so the ALTER really trains a dict.
        // Multi-row inserts keep the test fast.
        let body_fill = "abcdefghijklmnopqrstuvwxyz-".repeat(12);
        for chunk in 0..32 {
            let values: Vec<String> = (0..16)
                .map(|i| {
                    let id = chunk * 16 + i;
                    format!("({id}, 'log-line-{id}-{body_fill}')")
                })
                .collect();
            server
                .simple_query(&format!(
                    "insert into logs (id, body) values {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }
        server.force_checkpoint().await.unwrap();
        // Post-checkpoint: trains + persists the dictionary, rewrites files.
        server
            .simple_query("alter table logs set (compression = 'zstd')")
            .await
            .unwrap();
        for chunk in 32..36 {
            let values: Vec<String> = (0..16)
                .map(|i| {
                    let id = chunk * 16 + i;
                    format!("({id}, 'log-line-{id}-{body_fill}')")
                })
                .collect();
            server
                .simple_query(&format!(
                    "insert into logs (id, body) values {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }
        // No checkpoint here: recovery must replay CreateDictionary +
        // AlterTableCompression + dict-compressed FPIs from the WAL.
    }
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select count(*) from logs")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("576".to_string())]]);
    // The trained dictionary file exists.
    let dicts: Vec<_> = std::fs::read_dir(dir.path().join("dicts"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("dict"))
        .collect();
    assert_eq!(dicts.len(), 1);
}
