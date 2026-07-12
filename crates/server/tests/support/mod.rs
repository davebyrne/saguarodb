#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::Result;
use saguarodb_server::app::AppState;
use saguarodb_server::checkpoint::run_checkpoint;
use saguarodb_server::config::Config;
use saguarodb_server::connection::handle_connection;
use saguarodb_server::recovery::open_app;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

const READY_FOR_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TestServer {
    addr: SocketAddr,
    app: Arc<AppState>,
    accept_task: tokio::task::JoinHandle<()>,
    _temp_dir: Option<TempDir>,
}

impl TestServer {
    pub async fn start() -> Result<Self> {
        let temp_dir = tempfile::tempdir().map_err(|err| {
            common::DbError::io(format!("failed to create test data directory: {err}"))
        })?;
        let path = temp_dir.path().to_path_buf();
        Self::start_inner(&path, Some(temp_dir)).await
    }

    pub async fn start_with_data_dir(path: &Path) -> Result<Self> {
        Self::start_inner(path, None).await
    }

    /// Start with a caller-supplied [`Config`] (port is forced to an ephemeral one),
    /// for tests that need a specific checkpoint cadence or auto-vacuum threshold. If
    /// the config still carries the default `./data` dir a fresh temp dir is created
    /// and owned by the returned server; a caller-set `data_dir` is used as-is (so a
    /// restart test can reopen the same directory).
    pub async fn start_with_config(mut config: Config) -> Result<Self> {
        config.port = 0;
        if config.data_dir == Config::default().data_dir {
            let temp_dir = tempfile::tempdir().map_err(|err| {
                common::DbError::io(format!("failed to create test data directory: {err}"))
            })?;
            config.data_dir = temp_dir.path().to_path_buf();
            Self::start_inner_with_config(config, Some(temp_dir)).await
        } else {
            Self::start_inner_with_config(config, None).await
        }
    }

    /// The number of completed checkpoints (observability).
    pub fn checkpoint_count(&self) -> usize {
        self.app
            .components
            .checkpoint
            .checkpoints
            .load(std::sync::atomic::Ordering::Acquire) as usize
    }

    /// The current dead-versions-since-last-auto-prune accumulator (Milestone F4b).
    pub fn dead_rows_since_vacuum(&self) -> u64 {
        self.app
            .components
            .dead_rows_since_vacuum
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The full-extent heap page count for a table (resident + evicted pages), used
    /// to assert space stays bounded under churn. Resolves the table's heap
    /// `FileId` from the catalog's current storage id.
    pub fn heap_page_count(&self, table: &str) -> u32 {
        let schema = self
            .app
            .components
            .catalog
            .get_table_by_name(table)
            .expect("catalog lookup")
            .expect("table exists");
        self.app
            .components
            .buffer_pool
            .page_count(schema.storage_id)
            .expect("page count")
    }

    pub async fn simple_query(&self, sql: &str) -> Result<SimpleQueryResult> {
        let mut stream = TcpStream::connect(self.addr).await.map_err(|err| {
            common::DbError::io(format!("failed to connect to test server: {err}"))
        })?;
        stream
            .write_all(&startup_bytes())
            .await
            .map_err(|err| common::DbError::io(format!("failed to send startup message: {err}")))?;
        read_until_ready(&mut stream).await?;

        stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send query message: {err}")))?;
        let response = read_until_ready(&mut stream).await?;
        stream.write_all(&terminate_bytes()).await.map_err(|err| {
            common::DbError::io(format!("failed to send terminate message: {err}"))
        })?;

        decode_simple_query_response(&response)
    }

    pub async fn connect_raw(&self) -> Result<TcpStream> {
        TcpStream::connect(self.addr)
            .await
            .map_err(|err| common::DbError::io(format!("failed to connect to test server: {err}")))
    }

    /// Send a PostgreSQL `CancelRequest` for `(process_id, secret_key)` on its own
    /// fresh connection (as a real client does), so the server signals that
    /// connection's in-flight query to cancel.
    pub async fn send_cancel(&self, process_id: i32, secret_key: i32) -> Result<()> {
        let mut stream = TcpStream::connect(self.addr)
            .await
            .map_err(|err| common::DbError::io(format!("failed to connect for cancel: {err}")))?;
        let mut msg = Vec::with_capacity(16);
        msg.extend_from_slice(&16i32.to_be_bytes()); // length
        msg.extend_from_slice(&80877102i32.to_be_bytes()); // CancelRequest code (1234<<16|5678)
        msg.extend_from_slice(&process_id.to_be_bytes());
        msg.extend_from_slice(&secret_key.to_be_bytes());
        stream
            .write_all(&msg)
            .await
            .map_err(|err| common::DbError::io(format!("failed to send CancelRequest: {err}")))?;
        Ok(())
    }

    pub async fn force_checkpoint(&self) -> Result<()> {
        let app = self.app.clone();
        tokio::task::spawn_blocking(move || run_checkpoint(&app.components))
            .await
            .map_err(|err| common::DbError::internal(format!("checkpoint task failed: {err}")))?
    }

    /// Wait until normal storage has appended a heap insert for `table`. This is
    /// used to establish that a streaming COPY mutated a page before cancellation,
    /// rather than merely assuming a sent TCP frame was already consumed.
    pub async fn wait_for_heap_insert(&self, table: &str) -> Result<()> {
        let file_id = self
            .app
            .components
            .catalog
            .get_table_by_name(table)?
            .ok_or_else(|| common::DbError::internal(format!("table {table} is missing")))?
            .storage_id;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let found = self
                    .app
                    .components
                    .wal
                    .replay_from(0)?
                    .filter_map(|record| record.ok())
                    .any(|record| {
                        matches!(record.kind, WalRecordKind::HeapInsert { file_id: id, .. } if id == file_id)
                    });
                if found {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| common::DbError::internal("timed out waiting for heap insert"))?
    }

    /// The shared application state, for tests that inspect server internals such
    /// as the active-transaction registry.
    pub fn app(&self) -> &Arc<AppState> {
        &self.app
    }

    /// The number of currently in-progress transactions in the registry.
    pub fn active_txn_count(&self) -> usize {
        self.app.components.active_txns.active_ids().len()
    }

    async fn start_inner(path: &Path, temp_dir: Option<TempDir>) -> Result<Self> {
        let config = Config {
            data_dir: path.to_path_buf(),
            port: 0,
            buffer_pool_frames: 32,
            checkpoint_every_n_commits: 1_000,
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            shutdown_timeout_ms: 1_000,
            ..Config::default()
        };
        Self::start_inner_with_config(config, temp_dir).await
    }

    async fn start_inner_with_config(config: Config, temp_dir: Option<TempDir>) -> Result<Self> {
        let app = Arc::new(open_app(config)?);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|err| common::DbError::io(format!("failed to bind test server: {err}")))?;
        let addr = listener.local_addr().map_err(|err| {
            common::DbError::io(format!("failed to read test server address: {err}"))
        })?;
        let accept_app = app.clone();
        let accept_task = tokio::spawn(async move {
            while accept_app.components.shutdown.is_accepting() {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let app = accept_app.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(socket, app).await {
                        eprintln!("test connection failed: {err}");
                    }
                });
            }
        });

        Ok(Self {
            addr,
            app,
            accept_task,
            _temp_dir: temp_dir,
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

pub struct SimpleQueryResult {
    rows: Vec<Vec<Option<String>>>,
}

impl SimpleQueryResult {
    pub fn unwrap_rows(self) -> Vec<Vec<Option<String>>> {
        self.rows
    }
}

/// A persistent client connection that keeps one TCP stream open across queries,
/// so multi-statement transactions (`BEGIN ... COMMIT`) work as a single session.
/// Each query returns the decoded rows and the `ReadyForQuery` transaction-status
/// byte (`b'I'`/`b'T'`/`b'E'`).
pub struct Connection {
    stream: TcpStream,
    /// The `(process_id, secret_key)` from the startup `BackendKeyData`, used to
    /// target this connection's in-flight query with a `CancelRequest`.
    backend_key: (i32, i32),
}

impl Connection {
    /// Open and complete the startup handshake.
    pub async fn connect(server: &TestServer) -> Result<Self> {
        let mut stream = TcpStream::connect(server.addr).await.map_err(|err| {
            common::DbError::io(format!("failed to connect to test server: {err}"))
        })?;
        stream
            .write_all(&startup_bytes())
            .await
            .map_err(|err| common::DbError::io(format!("failed to send startup message: {err}")))?;
        let response = read_until_ready(&mut stream).await?;
        let backend_key = parse_backend_key(&response)?;
        Ok(Self {
            stream,
            backend_key,
        })
    }

    /// The connection's `(process_id, secret_key)` for issuing a `CancelRequest`.
    pub fn backend_key(&self) -> (i32, i32) {
        self.backend_key
    }

    /// Run one simple query on this connection, returning the decoded rows and the
    /// trailing `ReadyForQuery` status byte. Errors decode as a `DbError` carrying
    /// the server's error message.
    pub async fn query(&mut self, sql: &str) -> Result<QueryOutcome> {
        self.stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send query message: {err}")))?;
        let response = read_until_ready(&mut self.stream).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    /// Send one simple query and return the raw response bytes through the trailing
    /// `ReadyForQuery`, for tests that assert protocol framing.
    pub async fn query_raw(&mut self, sql: &str) -> Result<Vec<u8>> {
        self.stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send query message: {err}")))?;
        read_until_ready(&mut self.stream).await
    }

    /// Send a simple query and stop reading as soon as the first complete DataRow
    /// arrives, leaving the remaining result to exercise server-side backpressure.
    pub async fn begin_query_until_data_row(&mut self, sql: &str) -> Result<Vec<u8>> {
        self.stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send query: {err}")))?;
        read_until_tag_without_overread(&mut self.stream, b'D').await
    }

    /// Start a row-limited extended Execute without Sync and stop reading after
    /// its first complete DataRow, leaving the active portal fetch backpressured.
    pub async fn begin_extended_until_data_row(
        &mut self,
        sql: &str,
        max_rows: i32,
    ) -> Result<Vec<u8>> {
        let mut seq = parse_bytes("", sql, &[]);
        seq.extend(bind_bytes("", ""));
        seq.extend(execute_bytes_with_max_rows("", max_rows));
        self.stream
            .write_all(&seq)
            .await
            .map_err(|err| common::DbError::io(format!("failed to send extended query: {err}")))?;
        read_until_tag_without_overread(&mut self.stream, b'D').await
    }

    pub async fn send_extended_sync(&mut self) -> Result<()> {
        self.stream
            .write_all(&sync_bytes())
            .await
            .map_err(|err| common::DbError::io(format!("failed to send extended Sync: {err}")))
    }

    /// Resume reading a query after a deliberate pause, stopping at either a
    /// complete ReadyForQuery or connection close. The boolean is true on close.
    pub async fn read_until_ready_or_close(
        &mut self,
        timeout: Duration,
    ) -> Result<(Vec<u8>, bool)> {
        let mut response = Vec::new();
        let mut buf = [0; 8192];
        tokio::time::timeout(timeout, async {
            loop {
                let read = match self.stream.read(&mut buf).await {
                    Ok(read) => read,
                    Err(err)
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        return Ok((response, true));
                    }
                    Err(err) => {
                        return Err(common::DbError::io(format!(
                            "failed to read query response: {err}"
                        )));
                    }
                };
                if read == 0 {
                    return Ok((response, true));
                }
                response.extend_from_slice(&buf[..read]);
                if for_each_message(&response, |tag, _| tag == b'Z')? {
                    return Ok((response, false));
                }
            }
        })
        .await
        .map_err(|_| common::DbError::internal("timed out waiting for query cancellation"))?
    }

    /// Run a query expecting transport success; panics on protocol/transport
    /// error (a server SQL error is still returned in the `QueryOutcome`).
    pub async fn ok(&mut self, sql: &str) -> QueryOutcome {
        self.query(sql).await.expect("query transport failed")
    }

    /// Run one parameterless statement over the EXTENDED query protocol on this
    /// connection: send `Parse`/`Bind`/`Execute`/`Sync` (unnamed statement and
    /// portal, no parameters), then read until the trailing `ReadyForQuery`.
    /// Returns the decoded rows (or the server error) and the transaction-status
    /// byte from `ReadyForQuery`.
    pub async fn extended_execute(&mut self, sql: &str) -> Result<QueryOutcome> {
        let response = self.extended_execute_raw(sql).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    /// Run one parameterless extended-protocol statement and return the raw response
    /// bytes through `ReadyForQuery`.
    pub async fn extended_execute_raw(&mut self, sql: &str) -> Result<Vec<u8>> {
        let mut seq = parse_bytes("", sql, &[]);
        seq.extend(bind_bytes("", ""));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        self.extended_raw(seq).await
    }

    /// Run one extended-protocol statement with a single text parameter. The parse
    /// message leaves the parameter type unspecified (`0`), so the binder must infer
    /// it from context.
    pub async fn extended_execute_text_param(
        &mut self,
        sql: &str,
        param: &str,
    ) -> Result<QueryOutcome> {
        let mut seq = parse_bytes("", sql, &[0]);
        seq.extend(bind_text_param_bytes("", "", param));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        let response = self.extended_raw(seq).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    /// Send raw extended-protocol messages and return bytes through `ReadyForQuery`.
    pub async fn extended_raw(&mut self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.stream.write_all(&bytes).await.map_err(|err| {
            common::DbError::io(format!("failed to send extended-protocol sequence: {err}"))
        })?;
        read_until_ready(&mut self.stream).await
    }

    /// Start an unnamed, parameterless extended query and stop after its portal
    /// suspends, deliberately withholding Sync so the statement remains open.
    pub async fn begin_suspended_execute(&mut self, sql: &str, max_rows: i32) -> Result<()> {
        let mut seq = parse_bytes("", sql, &[]);
        seq.extend(bind_bytes("", ""));
        seq.extend(execute_bytes_with_max("", max_rows));
        self.stream.write_all(&seq).await.map_err(|err| {
            common::DbError::io(format!("failed to start suspended execute: {err}"))
        })?;
        read_until_tag(&mut self.stream, b's').await?;
        Ok(())
    }

    /// Start a named, parameterless portal and stop after suspension without Sync.
    /// Naming the portal lets later pipelined Bind messages coexist with it.
    pub async fn begin_named_suspended_execute(
        &mut self,
        statement: &str,
        portal: &str,
        sql: &str,
        max_rows: i32,
    ) -> Result<()> {
        let mut seq = parse_bytes(statement, sql, &[]);
        seq.extend(bind_bytes(portal, statement));
        seq.extend(execute_bytes_with_max(portal, max_rows));
        self.stream.write_all(&seq).await.map_err(|err| {
            common::DbError::io(format!("failed to start named suspended execute: {err}"))
        })?;
        read_until_tag(&mut self.stream, b's').await?;
        Ok(())
    }

    /// Pipeline `BEGIN` and one statement before Sync, preserving any named portal
    /// already suspended on the connection.
    pub async fn pipelined_begin_then_execute(&mut self, sql: &str) -> Result<QueryOutcome> {
        let mut seq = parse_bytes("pipelined_begin", "begin", &[]);
        seq.extend(bind_bytes("pipelined_begin_portal", "pipelined_begin"));
        seq.extend(execute_bytes("pipelined_begin_portal"));
        seq.extend(parse_bytes("pipelined_statement", sql, &[]));
        seq.extend(bind_bytes(
            "pipelined_statement_portal",
            "pipelined_statement",
        ));
        seq.extend(execute_bytes("pipelined_statement_portal"));
        seq.extend(sync_bytes());
        let response = self.extended_raw(seq).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    /// Resume an unnamed portal opened by [`Self::begin_suspended_execute`], run
    /// it to exhaustion, and Sync the extended protocol.
    pub async fn finish_suspended_execute(&mut self) -> Result<()> {
        let mut seq = execute_bytes("");
        seq.extend(sync_bytes());
        self.stream.write_all(&seq).await.map_err(|err| {
            common::DbError::io(format!("failed to resume suspended execute: {err}"))
        })?;
        read_until_ready(&mut self.stream).await?;
        Ok(())
    }

    pub fn extended_parse(sql: &str) -> Vec<u8> {
        parse_bytes("", sql, &[])
    }

    pub fn extended_bind() -> Vec<u8> {
        bind_bytes("", "")
    }

    pub fn extended_describe_statement(name: &str) -> Vec<u8> {
        describe_bytes(b'S', name)
    }

    pub fn extended_execute_portal() -> Vec<u8> {
        execute_bytes("")
    }

    pub fn extended_sync() -> Vec<u8> {
        sync_bytes()
    }

    pub async fn prepare(&mut self, name: &str, sql: &str) -> Result<QueryOutcome> {
        let mut seq = parse_bytes(name, sql, &[]);
        seq.extend(sync_bytes());
        self.stream.write_all(&seq).await.map_err(|err| {
            common::DbError::io(format!("failed to send extended-protocol parse: {err}"))
        })?;
        let response = read_until_ready(&mut self.stream).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    pub async fn execute_prepared(&mut self, name: &str) -> Result<QueryOutcome> {
        let mut seq = bind_bytes("", name);
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        self.stream.write_all(&seq).await.map_err(|err| {
            common::DbError::io(format!("failed to send extended-protocol execute: {err}"))
        })?;
        let response = read_until_ready(&mut self.stream).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    pub async fn execute_prepared_limited(
        &mut self,
        name: &str,
        max_rows: i32,
    ) -> Result<QueryOutcome> {
        let mut seq = bind_bytes("", name);
        seq.extend(execute_bytes_with_max("", max_rows));
        seq.extend(sync_bytes());
        self.stream.write_all(&seq).await.map_err(|err| {
            common::DbError::io(format!(
                "failed to send limited extended-protocol execute: {err}"
            ))
        })?;
        let response = read_until_ready(&mut self.stream).await?;
        let status = ready_for_query_status(&response)?;
        let result = decode_simple_query_response(&response);
        Ok(QueryOutcome { result, status })
    }

    /// Abruptly close the connection (drop the socket), simulating a client that
    /// disconnects mid-transaction.
    pub async fn close(self) {
        drop(self.stream);
    }

    /// Run `COPY ... FROM STDIN`: send the query, expect `CopyInResponse`, stream
    /// `chunks` as `CopyData`, then `CopyDone`. Returns the completion.
    pub async fn copy_from(&mut self, sql: &str, chunks: &[&[u8]]) -> Result<CopyCompletion> {
        self.copy_in(sql, chunks, None).await
    }

    /// Send `COPY ... FROM STDIN` and read the `CopyInResponse`, then return —
    /// leaving the COPY open (no `CopyDone`). The server holds its in-flight and
    /// writer guards until this connection sends a terminator or disconnects.
    pub async fn begin_copy_from(&mut self, sql: &str) -> Result<()> {
        self.stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send COPY query: {err}")))?;
        read_until_tag(&mut self.stream, b'G').await?;
        Ok(())
    }

    /// Send COPY FROM data frames without ending the sub-protocol.
    pub async fn send_copy_data(&mut self, chunks: &[&[u8]]) -> Result<()> {
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend_from_slice(&tagged(b'd', chunk));
        }
        self.stream
            .write_all(&out)
            .await
            .map_err(|err| common::DbError::io(format!("failed to send COPY data: {err}")))
    }

    /// Finish a COPY FROM that was opened with [`Self::begin_copy_from`], streaming
    /// the supplied chunks followed by `CopyDone`.
    pub async fn finish_copy_from(&mut self, chunks: &[&[u8]]) -> Result<CopyCompletion> {
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend_from_slice(&tagged(b'd', chunk));
        }
        out.extend_from_slice(&tagged(b'c', &[])); // CopyDone
        self.stream
            .write_all(&out)
            .await
            .map_err(|err| common::DbError::io(format!("failed to send COPY data: {err}")))?;

        let response = read_until_ready(&mut self.stream).await?;
        parse_copy_completion(&response)
    }

    /// Wait for an ErrorResponse emitted while COPY FROM remains in protocol drain
    /// mode (for example after a CancelRequest), without sending a COPY terminator.
    pub async fn wait_for_copy_error(&mut self) -> Result<common::DbError> {
        let response = read_until_tag(&mut self.stream, b'E').await?;
        match decode_simple_query_response(&response) {
            Err(err) => Ok(err),
            Ok(_) => Err(common::DbError::internal(
                "COPY response contained no decodable error",
            )),
        }
    }

    /// Like [`copy_from`](Self::copy_from) but aborts with `CopyFail(message)`
    /// instead of `CopyDone`.
    pub async fn copy_fail(
        &mut self,
        sql: &str,
        chunks: &[&[u8]],
        message: &str,
    ) -> Result<CopyCompletion> {
        self.copy_in(sql, chunks, Some(message)).await
    }

    async fn copy_in(
        &mut self,
        sql: &str,
        chunks: &[&[u8]],
        fail_message: Option<&str>,
    ) -> Result<CopyCompletion> {
        self.stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send COPY query: {err}")))?;
        // The server replies CopyInResponse ('G') before any ReadyForQuery.
        read_until_tag(&mut self.stream, b'G').await?;

        let mut out = Vec::new();
        for chunk in chunks {
            out.extend_from_slice(&tagged(b'd', chunk));
        }
        match fail_message {
            None => out.extend_from_slice(&tagged(b'c', &[])), // CopyDone
            Some(message) => {
                let mut body = message.as_bytes().to_vec();
                body.push(0);
                out.extend_from_slice(&tagged(b'f', &body)); // CopyFail
            }
        }
        self.stream
            .write_all(&out)
            .await
            .map_err(|err| common::DbError::io(format!("failed to send COPY data: {err}")))?;

        let response = read_until_ready(&mut self.stream).await?;
        parse_copy_completion(&response)
    }

    /// Run `COPY ... TO STDOUT`: returns the concatenated `CopyData` payload bytes
    /// and the completion (command tag / error / status).
    pub async fn copy_to(&mut self, sql: &str) -> Result<(Vec<u8>, CopyCompletion)> {
        self.stream
            .write_all(&query_bytes(sql))
            .await
            .map_err(|err| common::DbError::io(format!("failed to send COPY query: {err}")))?;
        let response = read_until_ready(&mut self.stream).await?;
        let data = extract_copy_data(&response);
        let completion = parse_copy_completion(&response)?;
        Ok((data, completion))
    }
}

/// The outcome of a COPY: the `CommandComplete` tag (e.g. `"COPY 2"`) on success,
/// or the error SQLSTATE on failure, plus the trailing transaction-status byte.
pub struct CopyCompletion {
    pub command_tag: Option<String>,
    pub error_code: Option<String>,
    pub error_count: usize,
    pub status: u8,
}

/// The result of one query on a persistent [`Connection`]: the decoded rows (or
/// an error) and the session's transaction-status byte afterward.
pub struct QueryOutcome {
    pub result: Result<SimpleQueryResult>,
    pub status: u8,
}

impl QueryOutcome {
    pub fn rows(self) -> Vec<Vec<Option<String>>> {
        self.result.expect("expected query success").rows
    }

    pub fn unwrap(self) -> SimpleQueryResult {
        self.result.expect("expected query success")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowDescriptionField {
    pub name: String,
    pub table_oid: i32,
    pub attr_num: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

pub fn first_row_description(bytes: &[u8]) -> Result<Vec<RowDescriptionField>> {
    let mut row_description = None;
    for_each_message(bytes, |tag, body| {
        if tag == b'T' {
            row_description = Some(body.to_vec());
            return true;
        }
        false
    })?;
    let body = row_description.ok_or_else(|| {
        common::DbError::protocol(
            common::SqlState::InternalError,
            "response did not include RowDescription",
        )
    })?;
    decode_row_description_body(&body)
}

pub fn command_tags(bytes: &[u8]) -> Result<Vec<String>> {
    let mut tags = Vec::new();
    for_each_message(bytes, |tag, body| {
        if tag == b'C' {
            tags.push(
                String::from_utf8_lossy(body.split(|&b| b == 0).next().unwrap_or(&[])).into_owned(),
            );
        }
        false
    })?;
    Ok(tags)
}

/// Extract the transaction-status byte from the trailing `ReadyForQuery` (`Z`)
/// message in a simple-query response.
fn ready_for_query_status(bytes: &[u8]) -> Result<u8> {
    let mut offset = 0;
    while offset < bytes.len() {
        let tag = bytes[offset];
        if tag == b'N' {
            offset += 1;
            continue;
        }
        if offset + 5 > bytes.len() {
            break;
        }
        let len = read_i32(&bytes[offset + 1..offset + 5])? as usize;
        let end = offset + 1 + len;
        if end > bytes.len() {
            break;
        }
        if tag == b'Z' && end == offset + 6 {
            return Ok(bytes[offset + 5]);
        }
        offset = end;
    }
    Err(common::DbError::protocol(
        common::SqlState::InternalError,
        "no ReadyForQuery status byte in response",
    ))
}

/// Append a durable but never-committed transaction's heap records to a fresh
/// WAL, on a standalone file id (`7`) that no table claims. Under redo-all
/// recovery (`docs/specs/mvcc.md` §8) these ARE replayed (reconstructing an orphan
/// page), but the transaction has no `Commit`, so it is recovered as aborted and
/// its tuple is invisible — and, being on file id 7, it never collides with a
/// table created after recovery (which starts at file id 1).
pub fn write_uncommitted_record_for_test(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|err| {
        common::DbError::io(format!(
            "failed to create test WAL directory {}: {err}",
            path.display()
        ))
    })?;
    let wal = FileWalManager::open(path.join("wal.dat"))?;
    wal.append(WalRecord {
        lsn: 0,
        txn_id: 1,
        kind: WalRecordKind::HeapInit {
            file_id: 7,
            page_num: 0,
        },
    })?;
    wal.append(WalRecord {
        lsn: 0,
        txn_id: 1,
        kind: WalRecordKind::HeapInsert {
            file_id: 7,
            page_num: 0,
            slot: 0,
            row_bytes: vec![1, 2, 3],
        },
    })?;
    wal.flush()?;
    Ok(())
}

/// Append a durable `CreateSchema` without a transaction outcome so recovery
/// must skip the object while still reserving its id.
pub fn write_uncommitted_schema_for_test(path: &Path, schema_id: u32) -> Result<()> {
    fs::create_dir_all(path).map_err(|err| {
        common::DbError::io(format!(
            "failed to create test WAL directory {}: {err}",
            path.display()
        ))
    })?;
    let wal = FileWalManager::open(path.join("wal.dat"))?;
    wal.append(WalRecord {
        lsn: 0,
        txn_id: common::FIRST_NORMAL_XID,
        kind: WalRecordKind::CreateSchema {
            schema: common::NamespaceSchema {
                id: schema_id,
                name: "crashed_schema".to_string(),
            },
        },
    })?;
    wal.flush()?;
    Ok(())
}

pub struct WorkspaceGraph {
    crates: BTreeMap<String, CrateManifest>,
}

impl WorkspaceGraph {
    pub fn load_from_manifest_dir(manifest_dir: &str) -> io::Result<Self> {
        let root = find_workspace_root(Path::new(manifest_dir))?;
        let root_manifest = fs::read_to_string(root.join("Cargo.toml"))?;
        let members = parse_workspace_members(&root_manifest);
        let mut crates = BTreeMap::new();

        for member in members {
            let path = root.join(member).join("Cargo.toml");
            let text = fs::read_to_string(&path)?;
            let manifest = parse_crate_manifest(&text, &path);
            crates.insert(manifest.package_name.clone(), manifest);
        }

        Ok(Self { crates })
    }

    pub fn depends_on(&self, from: &str, to: &str) -> bool {
        self.crates
            .get(from)
            .map(|manifest| manifest.dependencies.contains(to))
            .unwrap_or(false)
    }

    pub fn any_library_depends_on(&self, package: &str) -> bool {
        self.crates
            .values()
            .any(|manifest| manifest.is_library && manifest.dependencies.contains(package))
    }
}

struct CrateManifest {
    package_name: String,
    is_library: bool,
    dependencies: BTreeSet<String>,
}

async fn read_until_ready(stream: &mut TcpStream) -> Result<Vec<u8>> {
    read_until_ready_with_timeout(stream, READY_FOR_QUERY_TIMEOUT).await
}

pub(crate) async fn read_until_ready_with_timeout(
    stream: &mut TcpStream,
    timeout: Duration,
) -> Result<Vec<u8>> {
    tokio::time::timeout(timeout, read_until_ready_unbounded(stream))
        .await
        .map_err(|_| {
            common::DbError::internal(format!(
                "timed out waiting for ReadyForQuery after {} ms",
                timeout.as_millis()
            ))
        })?
}

async fn read_until_ready_unbounded(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut response = Vec::new();
    let mut buf = [0; 8192];
    loop {
        let read = stream
            .read(&mut buf)
            .await
            .map_err(|err| common::DbError::io(format!("failed to read response: {err}")))?;
        if read == 0 {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "connection closed before ReadyForQuery",
            ));
        }
        response.extend_from_slice(&buf[..read]);
        if response_contains_ready(&response)? {
            return Ok(response);
        }
    }
}

/// Read from `stream` until a complete message with tag `tag` is present (used to
/// wait for `CopyInResponse` (`G`), which precedes any `ReadyForQuery`).
async fn read_until_tag(stream: &mut TcpStream, tag: u8) -> Result<Vec<u8>> {
    let mut response = Vec::new();
    let mut buf = [0; 8192];
    let read_loop = async {
        loop {
            let read = stream.read(&mut buf).await.map_err(|err| {
                common::DbError::io(format!("failed to read COPY response: {err}"))
            })?;
            if read == 0 {
                return Err(common::DbError::protocol(
                    common::SqlState::InternalError,
                    "connection closed before expected COPY message",
                ));
            }
            response.extend_from_slice(&buf[..read]);
            if tag != b'E' && for_each_message(&response, |t, _| t == b'E')? {
                return match decode_simple_query_response(&response) {
                    Err(err) => Err(err),
                    Ok(_) => Err(common::DbError::internal(
                        "COPY startup returned an undecodable ErrorResponse",
                    )),
                };
            }
            if for_each_message(&response, |t, _| t == tag)? {
                return Ok(response);
            }
        }
    };
    tokio::time::timeout(READY_FOR_QUERY_TIMEOUT, read_loop)
        .await
        .map_err(|_| common::DbError::internal("timed out waiting for COPY message"))?
}

/// Variant used when the caller will resume reading the same response: reads one
/// byte at a time so it stops exactly on the requested frame boundary and does not
/// discard a partial following frame.
async fn read_until_tag_without_overread(stream: &mut TcpStream, tag: u8) -> Result<Vec<u8>> {
    let mut response = Vec::new();
    let mut byte = [0u8; 1];
    tokio::time::timeout(READY_FOR_QUERY_TIMEOUT, async {
        loop {
            let read = stream.read(&mut byte).await.map_err(|err| {
                common::DbError::io(format!("failed to read query response: {err}"))
            })?;
            if read == 0 {
                return Err(common::DbError::protocol(
                    common::SqlState::InternalError,
                    "connection closed before expected query message",
                ));
            }
            response.push(byte[0]);
            if for_each_message(&response, |t, _| t == b'E')? {
                return match decode_simple_query_response(&response) {
                    Err(err) => Err(err),
                    Ok(_) => Err(common::DbError::internal(
                        "query returned an undecodable ErrorResponse",
                    )),
                };
            }
            if for_each_message(&response, |t, _| t == tag)? {
                return Ok(response);
            }
        }
    })
    .await
    .map_err(|_| common::DbError::internal("timed out waiting for query message"))?
}

/// Visit each complete tagged message `(tag, body)` in `bytes`; returns `true` if
/// `visit` returns `true` for any, ignoring a trailing incomplete frame.
fn for_each_message(bytes: &[u8], mut visit: impl FnMut(u8, &[u8]) -> bool) -> Result<bool> {
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let tag = bytes[offset];
        let len = read_i32(&bytes[offset + 1..offset + 5])? as usize;
        let end = offset + 1 + len;
        if len < 4 || end > bytes.len() {
            break; // incomplete trailing frame
        }
        if visit(tag, &bytes[offset + 5..end]) {
            return Ok(true);
        }
        offset = end;
    }
    Ok(false)
}

/// Concatenate every `CopyData` (`d`) message body in a COPY-to-stdout response.
fn extract_copy_data(bytes: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    let _ = for_each_message(bytes, |tag, body| {
        if tag == b'd' {
            data.extend_from_slice(body);
        }
        false
    });
    data
}

/// Pull the `CommandComplete` tag and/or `ErrorResponse` SQLSTATE out of a COPY
/// response, with the trailing `ReadyForQuery` status byte.
fn parse_copy_completion(bytes: &[u8]) -> Result<CopyCompletion> {
    let mut command_tag = None;
    let mut error_code = None;
    let mut error_count = 0;
    let _ = for_each_message(bytes, |tag, body| {
        match tag {
            b'C' => {
                command_tag = Some(
                    String::from_utf8_lossy(body.split(|&b| b == 0).next().unwrap_or(&[]))
                        .into_owned(),
                );
            }
            b'E' => {
                error_count += 1;
                error_code = error_sqlstate(body);
            }
            _ => {}
        }
        false
    });
    Ok(CopyCompletion {
        command_tag,
        error_code,
        error_count,
        status: ready_for_query_status(bytes)?,
    })
}

/// Extract the SQLSTATE (`C`) field from an `ErrorResponse` body.
fn error_sqlstate(body: &[u8]) -> Option<String> {
    let mut offset = 0;
    while offset < body.len() && body[offset] != 0 {
        let field_type = body[offset];
        let start = offset + 1;
        let nul = start + body[start..].iter().position(|&b| b == 0)?;
        if field_type == b'C' {
            return Some(String::from_utf8_lossy(&body[start..nul]).into_owned());
        }
        offset = nul + 1;
    }
    None
}

fn response_contains_ready(bytes: &[u8]) -> Result<bool> {
    let mut offset = 0;
    while offset < bytes.len() {
        let tag = bytes[offset];
        if tag == b'N' {
            offset += 1;
            continue;
        }
        if offset + 5 > bytes.len() {
            return Ok(false);
        }
        let len = read_i32(&bytes[offset + 1..offset + 5])?;
        if len < 4 {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "server message length is too short",
            ));
        }
        let end = offset + 1 + len as usize;
        if bytes.len() < end {
            return Ok(false);
        }
        if tag == b'Z' {
            return Ok(true);
        }
        offset = end;
    }
    Ok(false)
}

fn decode_simple_query_response(bytes: &[u8]) -> Result<SimpleQueryResult> {
    let mut offset = 0;
    let mut rows = Vec::new();
    while offset < bytes.len() {
        let tag = bytes[offset];
        if offset + 5 > bytes.len() {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "truncated server message",
            ));
        }
        let len = read_i32(&bytes[offset + 1..offset + 5])?;
        if len < 4 {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "server message length is too short",
            ));
        }
        let body_start = offset + 5;
        let body_end = offset + 1 + len as usize;
        if body_end > bytes.len() {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "truncated server message body",
            ));
        }
        let body = &bytes[body_start..body_end];
        match tag {
            b'D' => rows.push(decode_data_row(body)?),
            b'E' => {
                return Err(decode_error_response(body));
            }
            // Simple-query framing tags plus the extended-protocol acknowledgements
            // (`1` ParseComplete, `2` BindComplete, `n` NoData, `t`
            // ParameterDescription) that carry no rows.
            b'T' | b'C' | b'Z' | b'S' | b'1' | b'2' | b'n' | b't' => {}
            _ => {
                return Err(common::DbError::protocol(
                    common::SqlState::InternalError,
                    format!("unexpected server message tag {}", tag as char),
                ));
            }
        }
        offset = body_end;
    }
    Ok(SimpleQueryResult { rows })
}

fn decode_data_row(body: &[u8]) -> Result<Vec<Option<String>>> {
    if body.len() < 2 {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "data row missing column count",
        ));
    }
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > body.len() {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "data row missing value length",
            ));
        }
        let len = read_i32(&body[offset..offset + 4])?;
        offset += 4;
        if len == -1 {
            values.push(None);
            continue;
        }
        if len < 0 {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "data row value length is invalid",
            ));
        }
        let end = offset + len as usize;
        if end > body.len() {
            return Err(common::DbError::protocol(
                common::SqlState::InternalError,
                "data row value is truncated",
            ));
        }
        let value = std::str::from_utf8(&body[offset..end])
            .map_err(|_| {
                common::DbError::protocol(
                    common::SqlState::InternalError,
                    "data row value is not UTF-8",
                )
            })?
            .to_string();
        values.push(Some(value));
        offset = end;
    }
    Ok(values)
}

fn decode_error_message(body: &[u8]) -> String {
    let mut fields = Vec::new();
    let mut offset = 0;
    while offset < body.len() {
        let field = body[offset];
        if field == 0 {
            break;
        }
        offset += 1;
        let Some(relative_nul) = body[offset..].iter().position(|byte| *byte == 0) else {
            break;
        };
        let end = offset + relative_nul;
        let value = String::from_utf8_lossy(&body[offset..end]).to_string();
        fields.push(format!("{}={value}", field as char));
        offset = end + 1;
    }
    fields.join(", ")
}

fn decode_error_response(body: &[u8]) -> common::DbError {
    let mut sqlstate = common::SqlState::InternalError;
    let mut offset = 0;
    while offset < body.len() {
        let field = body[offset];
        if field == 0 {
            break;
        }
        offset += 1;
        let Some(relative_nul) = body[offset..].iter().position(|byte| *byte == 0) else {
            break;
        };
        let end = offset + relative_nul;
        if field == b'C'
            && let Ok(code) = std::str::from_utf8(&body[offset..end])
        {
            sqlstate = common::SqlState::from_code(code).unwrap_or(common::SqlState::InternalError);
        }
        offset = end + 1;
    }
    common::DbError::protocol(sqlstate, decode_error_message(body))
}

fn startup_bytes() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes());
    body.extend_from_slice(b"user\0saguarodb\0");
    body.extend_from_slice(b"database\0saguarodb\0");
    body.push(0);

    let mut packet = Vec::new();
    packet.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    packet.extend_from_slice(&body);
    packet
}

fn query_bytes(sql: &str) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(b'Q');
    packet.extend_from_slice(&(sql.len() as i32 + 5).to_be_bytes());
    packet.extend_from_slice(sql.as_bytes());
    packet.push(0);
    packet
}

fn terminate_bytes() -> Vec<u8> {
    vec![b'X', 0, 0, 0, 4]
}

/// Frame a message body with its one-byte tag and four-byte length prefix.
fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![tag];
    packet.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
    packet.extend_from_slice(body);
    packet
}

/// A `Parse` message: prepared-statement name, query text, and parameter type
/// OIDs (`0` = unspecified).
fn parse_bytes(name: &str, query: &str, param_oids: &[i32]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(query.as_bytes());
    body.push(0);
    body.extend_from_slice(&i16::try_from(param_oids.len()).unwrap().to_be_bytes());
    for oid in param_oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    tagged(b'P', &body)
}

/// A `Bind` message binding `statement` into `portal` with no parameter format
/// codes, no parameters, and no result format codes (all text).
fn bind_bytes(portal: &str, statement: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(&0i16.to_be_bytes()); // parameter format codes
    body.extend_from_slice(&0i16.to_be_bytes()); // parameters
    body.extend_from_slice(&0i16.to_be_bytes()); // result format codes
    tagged(b'B', &body)
}

fn bind_text_param_bytes(portal: &str, statement: &str, param: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(&0i16.to_be_bytes()); // parameter format codes (all text)
    body.extend_from_slice(&1i16.to_be_bytes()); // parameters
    body.extend_from_slice(&i32::try_from(param.len()).unwrap().to_be_bytes());
    body.extend_from_slice(param.as_bytes());
    body.extend_from_slice(&0i16.to_be_bytes()); // result format codes
    tagged(b'B', &body)
}

fn describe_bytes(kind: u8, name: &str) -> Vec<u8> {
    let mut body = vec![kind];
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    tagged(b'D', &body)
}

/// An `Execute` message for `portal` with no row limit (all rows).
fn execute_bytes(portal: &str) -> Vec<u8> {
    execute_bytes_with_max_rows(portal, 0)
}

fn execute_bytes_with_max_rows(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&max_rows.to_be_bytes());
    tagged(b'E', &body)
}

fn execute_bytes_with_max(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&max_rows.to_be_bytes());
    tagged(b'E', &body)
}

fn sync_bytes() -> Vec<u8> {
    tagged(b'S', &[])
}

/// Scan a startup response for the `BackendKeyData` (`K`) message and return its
/// `(process_id, secret_key)`. Mirrors `ready_for_query_status`'s message framing.
fn parse_backend_key(bytes: &[u8]) -> Result<(i32, i32)> {
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let tag = bytes[offset];
        if tag == b'N' {
            offset += 1;
            continue;
        }
        let len = read_i32(&bytes[offset + 1..offset + 5])? as usize;
        let end = offset + 1 + len;
        if end > bytes.len() {
            break;
        }
        if tag == b'K' && len == 12 {
            let body = &bytes[offset + 5..end];
            return Ok((read_i32(&body[0..4])?, read_i32(&body[4..8])?));
        }
        offset = end;
    }
    Err(common::DbError::protocol(
        common::SqlState::InternalError,
        "no BackendKeyData in startup response",
    ))
}

fn read_i32(bytes: &[u8]) -> Result<i32> {
    if bytes.len() != 4 {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "expected four-byte integer",
        ));
    }
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn decode_row_description_body(body: &[u8]) -> Result<Vec<RowDescriptionField>> {
    let mut offset = 0;
    let count = read_i16_at(body, &mut offset)? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_cstr_at(body, &mut offset)?;
        fields.push(RowDescriptionField {
            name,
            table_oid: read_i32_at(body, &mut offset)?,
            attr_num: read_i16_at(body, &mut offset)?,
            type_oid: read_i32_at(body, &mut offset)?,
            type_size: read_i16_at(body, &mut offset)?,
            type_modifier: read_i32_at(body, &mut offset)?,
            format_code: read_i16_at(body, &mut offset)?,
        });
    }
    if offset != body.len() {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "RowDescription has trailing bytes",
        ));
    }
    Ok(fields)
}

fn read_i16_at(bytes: &[u8], offset: &mut usize) -> Result<i16> {
    if *offset + 2 > bytes.len() {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "expected two-byte integer",
        ));
    }
    let value = i16::from_be_bytes([bytes[*offset], bytes[*offset + 1]]);
    *offset += 2;
    Ok(value)
}

fn read_i32_at(bytes: &[u8], offset: &mut usize) -> Result<i32> {
    if *offset + 4 > bytes.len() {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "expected four-byte integer",
        ));
    }
    let value = read_i32(&bytes[*offset..*offset + 4])?;
    *offset += 4;
    Ok(value)
}

fn read_cstr_at(bytes: &[u8], offset: &mut usize) -> Result<String> {
    let Some(relative_nul) = bytes[*offset..].iter().position(|byte| *byte == 0) else {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "unterminated string in RowDescription",
        ));
    };
    let end = *offset + relative_nul;
    let value = std::str::from_utf8(&bytes[*offset..end])
        .map_err(|_| {
            common::DbError::protocol(
                common::SqlState::InternalError,
                "RowDescription field name is not UTF-8",
            )
        })?
        .to_string();
    *offset = end + 1;
    Ok(value)
}

fn find_workspace_root(start: &Path) -> io::Result<PathBuf> {
    for ancestor in start.ancestors() {
        let manifest = ancestor.join("Cargo.toml");
        if manifest.exists() && fs::read_to_string(&manifest)?.contains("[workspace]") {
            return Ok(ancestor.to_path_buf());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "workspace root Cargo.toml not found",
    ))
}

fn parse_workspace_members(manifest: &str) -> Vec<String> {
    let mut members = Vec::new();
    let mut in_members = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("members") && trimmed.contains('[') {
            in_members = true;
            continue;
        }
        if in_members && trimmed.starts_with(']') {
            break;
        }
        if in_members {
            let member = trimmed.trim_matches(',').trim_matches('"');
            if !member.is_empty() {
                members.push(member.to_string());
            }
        }
    }
    members
}

fn parse_crate_manifest(manifest: &str, path: &Path) -> CrateManifest {
    let package_name = parse_package_name(manifest)
        .unwrap_or_else(|| panic!("manifest {} is missing package name", path.display()));
    let is_library = manifest.contains("[lib]")
        || path
            .parent()
            .map(|dir| dir.join("src/lib.rs").exists())
            .unwrap_or(false);
    let dependencies = parse_dependency_package_names(manifest);
    CrateManifest {
        package_name,
        is_library,
        dependencies,
    }
}

fn parse_package_name(manifest: &str) -> Option<String> {
    let mut in_package = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        match trimmed {
            "[package]" => in_package = true,
            line if line.starts_with('[') => in_package = false,
            line if in_package && line.starts_with("name") => {
                return quoted_value(line).map(str::to_string);
            }
            _ => {}
        }
    }
    None
}

fn parse_dependency_package_names(manifest: &str) -> BTreeSet<String> {
    let mut dependencies = BTreeSet::new();
    let mut in_inline_dependencies = false;
    let mut table_dependency: Option<(String, Option<String>)> = None;

    for line in manifest.lines().chain(std::iter::once("[end]")) {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            if let Some((alias, package)) = table_dependency.take() {
                dependencies.insert(package.unwrap_or_else(|| alias_package_name(&alias)));
            }
            in_inline_dependencies = false;

            if trimmed == "[dependencies]" {
                in_inline_dependencies = true;
                continue;
            }
            if let Some(alias) = dependency_table_alias(trimmed) {
                table_dependency = Some((alias.to_string(), None));
            }
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if in_inline_dependencies {
            let Some((alias, value)) = trimmed.split_once('=') else {
                continue;
            };
            let alias = alias.trim();
            let package_name = if let Some(package) = package_value(value) {
                package
            } else {
                alias_package_name(alias)
            };
            dependencies.insert(package_name);
        } else if let Some((_alias, package)) = table_dependency.as_mut()
            && let Some((key, value)) = trimmed.split_once('=')
            && key.trim() == "package"
        {
            *package = quoted_value(value).map(str::to_string);
        }
    }
    dependencies
}

fn dependency_table_alias(header: &str) -> Option<&str> {
    header
        .strip_prefix("[dependencies.")
        .and_then(|name| name.strip_suffix(']'))
        .map(|name| name.trim_matches('"'))
        .filter(|name| !name.is_empty())
}

fn package_value(value: &str) -> Option<String> {
    let package_start = value.find("package")?;
    quoted_value(&value[package_start..]).map(str::to_string)
}

fn alias_package_name(alias: &str) -> String {
    if alias.starts_with("saguarodb-") {
        alias.to_string()
    } else {
        format!("saguarodb-{alias}")
    }
}

fn quoted_value(line: &str) -> Option<&str> {
    let start = line.find('"')? + 1;
    let end = line[start..].find('"')? + start;
    Some(&line[start..end])
}
