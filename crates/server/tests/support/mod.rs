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

    pub async fn force_checkpoint(&self) -> Result<()> {
        let app = self.app.clone();
        tokio::task::spawn_blocking(move || run_checkpoint(&app.components))
            .await
            .map_err(|err| common::DbError::internal(format!("checkpoint task failed: {err}")))?
    }

    async fn start_inner(path: &Path, temp_dir: Option<TempDir>) -> Result<Self> {
        let config = Config {
            data_dir: path.to_path_buf(),
            port: 0,
            buffer_pool_frames: 32,
            checkpoint_every_n_commits: 1_000,
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            shutdown_timeout_ms: 1_000,
        };
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
        kind: WalRecordKind::HeapInsert {
            file_id: 1,
            page_num: 0,
            slot: 0,
            row_bytes: vec![1, 2, 3],
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
                return Err(common::DbError::protocol(
                    common::SqlState::InternalError,
                    decode_error_message(body),
                ));
            }
            b'T' | b'C' | b'Z' => {}
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

fn read_i32(bytes: &[u8]) -> Result<i32> {
    if bytes.len() != 4 {
        return Err(common::DbError::protocol(
            common::SqlState::InternalError,
            "expected four-byte integer",
        ));
    }
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
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
