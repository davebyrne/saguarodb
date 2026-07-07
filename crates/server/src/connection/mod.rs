use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{
    ColumnInfo, DbError, IsolationLevel, PgType, Result, Row, SessionInfo, SessionSequenceState,
    SessionState, SqlState, Value,
};
use protocol::{
    ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
    ServerMessage,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::app::AppState;
use crate::cancel::BackendKey;
use crate::query::{
    CopyInChunk, PreparedStatement, QuerySessionContext, SessionGucs, SessionTxnStatus,
    StreamOutcome, Transaction, abort_session_transaction,
};
use crate::session_registry::SessionActivityRecord;
use crate::shutdown::InFlightQueryGuard;

mod copy;
mod extended;
mod simple;

/// State for an in-progress `COPY ... FROM STDIN`. The blocking task owns the
/// transaction and inserts rows pulled from `sender`; the connection loop forwards
/// `CopyData` into it and finalizes on `CopyDone`/`CopyFail`/disconnect.
struct CopyInSession {
    sender: mpsc::Sender<CopyInChunk>,
    task: JoinHandle<(Option<Transaction>, Result<u64>)>,
    /// Set once the insert task has exited early on a row error: we then discard
    /// further `CopyData` and report the task's error on the terminator.
    insert_failed: bool,
    /// Keeps the COPY counted as an in-flight query for its whole streaming
    /// lifetime, so graceful shutdown's `wait_for_idle` accounts for it (the insert
    /// task holds the shared writer guard, which the final checkpoint must drain).
    /// Held until the task is awaited in `finish_copy_in` / dropped on disconnect.
    _guard: InFlightQueryGuard,
}

/// Accept a connection, run optional SSL/GSS negotiation, then serve the
/// protocol over the resulting (plaintext or TLS) stream.
///
/// Before startup a client may send a `GSSENCRequest` and/or an `SSLRequest`.
/// GSSAPI transport encryption is unsupported, so it is declined with a single
/// `N` byte and negotiation continues. For an `SSLRequest`, when the server has
/// TLS configured it replies `SslAccepted` (`S`) and upgrades the socket;
/// otherwise it replies `SslRejected` (`N`) and the client continues in
/// plaintext. A client that opens with a `StartupMessage` is served in plaintext
/// directly.
pub async fn handle_connection(mut socket: TcpStream, app: Arc<AppState>) -> Result<()> {
    let mut codec = PostgresCodec::new();
    let mut buf = [0; 8192];

    loop {
        // Read until the first client message of this negotiation step is
        // buffered. Looping keeps negotiation correct even when the small
        // request packet is split across reads.
        let initial = loop {
            let read = socket
                .read(&mut buf)
                .await
                .map_err(|err| DbError::io(format!("failed to read socket: {err}")))?;
            if read == 0 {
                return Ok(());
            }
            match codec.decode(&buf[..read]) {
                Ok(messages) if !messages.is_empty() => break messages,
                Ok(_) => continue,
                Err(err) => {
                    // Pre-startup: no session exists yet, and a fresh connection
                    // is necessarily idle.
                    write_messages(
                        &mut socket,
                        &codec,
                        &[
                            error_response(&err),
                            ServerMessage::ReadyForQuery(TransactionState::Idle.status_byte()),
                        ],
                    )
                    .await?;
                    return Ok(());
                }
            }
        };

        // A negotiation request must arrive alone; the client waits for the
        // single-byte reply before sending anything else.
        let is_negotiation = matches!(
            initial.first(),
            Some(ClientMessage::GssEncRequest | ClientMessage::SslRequest)
        );
        if is_negotiation && initial.len() > 1 {
            let err = DbError::protocol(
                SqlState::SyntaxError,
                "client sent data before completing connection negotiation",
            );
            // Pre-startup: no session exists yet, and a fresh connection is idle.
            write_messages(
                &mut socket,
                &codec,
                &[
                    error_response(&err),
                    ServerMessage::ReadyForQuery(TransactionState::Idle.status_byte()),
                ],
            )
            .await?;
            return Ok(());
        }

        match initial.first() {
            // A CancelRequest arrives on its own connection: signal the target
            // backend (if the key matches a live query) and close without reply.
            Some(ClientMessage::CancelRequest {
                process_id,
                secret_key,
            }) => {
                app.components.cancel_registry.request_cancel(BackendKey {
                    process_id: *process_id,
                    secret_key: *secret_key,
                });
                return Ok(());
            }
            // GSSAPI transport encryption is unsupported: decline with the single
            // `N` byte (same as SSL rejection) and keep negotiating, since the
            // client typically follows with an SSLRequest or StartupMessage.
            Some(ClientMessage::GssEncRequest) => {
                write_messages(&mut socket, &codec, &[ServerMessage::SslRejected]).await?;
                continue;
            }
            Some(ClientMessage::SslRequest) => {
                // Clone the acceptor (a cheap `Arc`) so `app` stays free to move
                // into `serve`.
                return match app.components.tls.clone() {
                    Some(acceptor) => {
                        write_messages(&mut socket, &codec, &[ServerMessage::SslAccepted]).await?;
                        socket.flush().await.map_err(|err| {
                            DbError::io(format!("failed to flush SSL response: {err}"))
                        })?;
                        let tls = acceptor
                            .accept(socket)
                            .await
                            .map_err(|err| DbError::io(format!("TLS handshake failed: {err}")))?;
                        // Serve the encrypted session with a fresh codec: only the
                        // lone SSLRequest is legitimate before the handshake, so a
                        // new decode buffer ensures no stray pre-handshake
                        // plaintext can bleed into the decrypted stream.
                        serve(tls, PostgresCodec::new(), app, Vec::new()).await
                    }
                    None => {
                        write_messages(&mut socket, &codec, &[ServerMessage::SslRejected]).await?;
                        serve(socket, codec, app, Vec::new()).await
                    }
                };
            }
            _ => {}
        }

        return serve(socket, codec, app, initial).await;
    }
}

/// A bound portal: a prepared statement plus its parameter values and the
/// requested result column formats.
struct Portal {
    statement: Arc<PreparedStatement>,
    params: Vec<Value>,
    result_formats: Vec<i16>,
}

/// The PostgreSQL transaction-block status reported in `ReadyForQuery`. Each
/// variant maps to the wire status byte the protocol encodes. The session's
/// transaction slot drives the transitions: `Idle` (`b'I'`) with no open
/// transaction, `InTransaction` (`b'T'`) inside a healthy block, and `Failed`
/// (`b'E'`) after a statement error until the block is ended
/// (`docs/specs/mvcc.md` §7.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionState {
    /// Not in a transaction block (autocommit). Status byte `b'I'`.
    Idle,
    /// In a live transaction block. Status byte `b'T'`.
    InTransaction,
    /// In a failed transaction block: rejects all but COMMIT/ROLLBACK. Status
    /// byte `b'E'`.
    Failed,
}

impl TransactionState {
    /// The PostgreSQL `ReadyForQuery` transaction-status byte for this state.
    pub fn status_byte(self) -> u8 {
        match self {
            TransactionState::Idle => b'I',
            TransactionState::InTransaction => b'T',
            TransactionState::Failed => b'E',
        }
    }
}

impl From<SessionTxnStatus> for TransactionState {
    fn from(status: SessionTxnStatus) -> Self {
        match status {
            SessionTxnStatus::Idle => TransactionState::Idle,
            SessionTxnStatus::InTransaction => TransactionState::InTransaction,
            SessionTxnStatus::Failed => TransactionState::Failed,
        }
    }
}

/// Per-connection state for the simple and extended query protocols.
struct Session {
    app: Arc<AppState>,
    state: PostgresConnectionState,
    prepared: HashMap<String, Arc<PreparedStatement>>,
    portals: HashMap<String, Portal>,
    /// Set after an error inside an extended-query sequence; subsequent extended
    /// messages are skipped until the client sends `Sync`.
    failed: bool,
    /// Transaction-block status reported in `ReadyForQuery`, derived from `txn`
    /// after each simple query and kept in sync so `ReadyForQuery` reports the
    /// right `b'I'`/`b'T'`/`b'E'` byte.
    tx: TransactionState,
    /// The open explicit transaction, threaded across simple queries. `None` in
    /// autocommit. Aborted on disconnect (`Drop`) so a client that disconnects
    /// mid-transaction does not leak the write guard or a registry entry.
    txn: Option<Transaction>,
    /// This connection's default isolation level for new transactions
    /// (`docs/specs/mvcc.md` §10 Milestone G2). Starts at `ReadCommitted` and is
    /// updated by `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL
    /// <level>`. Threaded in/out of the query path alongside `txn`: a `BEGIN` with no
    /// explicit `ISOLATION LEVEL` inherits it, while committed default changes update
    /// it. It persists across transactions on this connection and resets to
    /// `ReadCommitted` for each new connection (this field is per-`Session`).
    default_isolation: IsolationLevel,
    /// Per-connection sequence state for `currval`. `nextval` and
    /// `setval(..., true)` record into this map; `currval` reads it and errors
    /// before any value is recorded on this connection.
    session_sequences: Arc<SessionSequenceState>,
    /// Connection identity from startup plus BackendKeyData, used by PostgreSQL
    /// system information functions.
    session_info: Arc<SessionInfo>,
    /// Per-connection session configuration parameters used by driver startup
    /// probes (`SET`/`SHOW`/`RESET`/`DISCARD ALL`).
    session_gucs: Arc<SessionGucs>,
    /// The `application_name` value last reported to the client via
    /// `ParameterStatus` (startup report or a later change report).
    reported_application_name: String,
    /// Shared with the running query's `ExecutionContext`; set from another
    /// connection's `CancelRequest` to abort the in-flight query.
    cancel: Arc<AtomicBool>,
    /// This connection's cancellation key, registered at startup and removed on
    /// disconnect.
    backend_key: Option<BackendKey>,
    /// Registered activity row backing `pg_stat_activity` after startup.
    activity: Option<Arc<SessionActivityRecord>>,
    /// Set while a `COPY ... FROM STDIN` is streaming: subsequent client messages
    /// are routed as copy-in data until `CopyDone`/`CopyFail`. On disconnect this
    /// drops, closing the channel so the blocking task aborts the COPY.
    copy_in: Option<CopyInSession>,
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(key) = self.backend_key {
            self.app.components.cancel_registry.deregister(key);
        }
        if let Some(activity) = self.activity.take() {
            self.app.components.session_registry.deregister(&activity);
        }
        // A client that disconnected mid-transaction leaves an open transaction:
        // abort it so the exclusive write guard and the registry entry are not
        // leaked. The abort is in-memory before-image undo plus an (unflushed)
        // Abort record — brief and bounded, safe to run during drop.
        if let Some(txn) = self.txn.take() {
            abort_session_transaction(&self.app.components, txn);
        }
    }
}

/// Drive the protocol over an established stream, starting with any messages
/// already decoded during negotiation. Generic over the stream type so it serves
/// both plaintext `TcpStream` and TLS-upgraded connections.
async fn serve<S>(
    mut stream: S,
    mut codec: PostgresCodec,
    app: Arc<AppState>,
    mut batch: Vec<ClientMessage>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = Session::new(app);
    let mut buf = [0; 8192];

    loop {
        for message in batch {
            if session
                .handle(&mut stream, &codec, message)
                .await?
                .is_break()
            {
                return Ok(());
            }
        }

        let read = stream
            .read(&mut buf)
            .await
            .map_err(|err| DbError::io(format!("failed to read socket: {err}")))?;
        if read == 0 {
            return Ok(());
        }
        batch = match codec.decode(&buf[..read]) {
            Ok(messages) => messages,
            Err(err) => {
                write_messages(
                    &mut stream,
                    &codec,
                    &[
                        error_response(&err),
                        ServerMessage::ReadyForQuery(session.status_byte()),
                    ],
                )
                .await?;
                return Ok(());
            }
        };
    }
}

impl Session {
    fn new(app: Arc<AppState>) -> Self {
        Self {
            app,
            state: PostgresConnectionState::new(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            failed: false,
            tx: TransactionState::Idle,
            txn: None,
            // A fresh connection defaults to Read Committed (Postgres' default),
            // regardless of any other connection's session setting (`docs/specs/mvcc.md`
            // §10 Milestone G2).
            default_isolation: IsolationLevel::default(),
            session_sequences: Arc::new(SessionSequenceState::new()),
            session_info: Arc::new(SessionInfo::default()),
            session_gucs: Arc::new(SessionGucs::default()),
            reported_application_name: String::new(),
            cancel: Arc::new(AtomicBool::new(false)),
            backend_key: None,
            activity: None,
            copy_in: None,
        }
    }

    /// The `ReadyForQuery` transaction-status byte for the session's current
    /// transaction state.
    fn status_byte(&self) -> u8 {
        self.tx.status_byte()
    }

    /// Clear the cancellation flag and hand a shared clone to the query about to
    /// run, so a `CancelRequest` received during execution aborts it (and a
    /// cancellation requested between queries is ignored).
    fn begin_cancelable(&self) -> Arc<AtomicBool> {
        self.cancel.store(false, Ordering::Relaxed);
        self.cancel.clone()
    }

    fn query_session_context(&self, cancel: Arc<AtomicBool>) -> QuerySessionContext {
        QuerySessionContext::new(
            cancel,
            self.session_sequences.clone(),
            self.session_info.clone(),
            self.session_gucs.clone(),
        )
        .with_session_registry(self.app.components.session_registry.clone())
    }

    fn begin_activity(&self, query: &str) {
        if let Some(activity) = &self.activity {
            activity.begin_statement(query);
        }
    }

    fn end_activity(&self) {
        let state = match self.tx {
            TransactionState::Idle => SessionState::Idle,
            TransactionState::InTransaction => SessionState::IdleInTransaction,
            TransactionState::Failed => SessionState::IdleInTransactionAborted,
        };
        if let Some(activity) = &self.activity {
            activity.end_statement(state);
        }
    }

    /// Report `application_name` changes after `SET`/`RESET`/`DISCARD ALL`.
    /// Other startup-reported parameters are fixed in this server.
    fn application_name_status_change(&mut self) -> Option<ServerMessage> {
        let current = self.session_gucs.application_name();
        if current == self.reported_application_name {
            return None;
        }
        self.reported_application_name = current.clone();
        Some(ServerMessage::ParameterStatus {
            key: "application_name".to_string(),
            value: current,
        })
    }

    /// Handle one decoded client message. Returns `Break` when the connection
    /// should close (Terminate, or a shutdown-rejected simple query).
    async fn handle<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        message: ClientMessage,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        // While a COPY FROM is streaming, only copy-in messages are valid; anything
        // else is a protocol violation.
        if self.copy_in.is_some() {
            match message {
                ClientMessage::CopyData(bytes) => self.handle_copy_data(bytes).await?,
                ClientMessage::CopyDone => self.finish_copy_in(stream, codec, None).await?,
                ClientMessage::CopyFail(message) => {
                    self.finish_copy_in(stream, codec, Some(message)).await?
                }
                // The client disconnected mid-COPY; the session drop aborts it.
                ClientMessage::Terminate => return Ok(ControlFlow::Break(())),
                _ => {
                    return Err(protocol_error(
                        "expected COPY data while a COPY FROM STDIN is in progress",
                    ));
                }
            }
            return Ok(ControlFlow::Continue(()));
        }
        match message {
            ClientMessage::Query(sql) => return self.run_query(stream, codec, sql).await,
            ClientMessage::Sync => {
                self.failed = false;
                write_messages(
                    stream,
                    codec,
                    &[ServerMessage::ReadyForQuery(self.status_byte())],
                )
                .await?;
            }
            ClientMessage::Flush => {
                stream
                    .flush()
                    .await
                    .map_err(|err| DbError::io(format!("failed to flush socket: {err}")))?;
            }
            ClientMessage::Parse {
                name,
                query,
                param_types,
            } if !self.failed => {
                let result = self.process_parse(name, query, &param_types);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } if !self.failed => {
                let result =
                    self.process_bind(portal, &statement, &param_formats, params, result_formats);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Describe { kind, name } if !self.failed => {
                let result = self.process_describe(kind, &name);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Close { kind, name } if !self.failed => {
                let result = self.process_close(kind, &name);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Execute { portal, .. } if !self.failed => {
                self.run_execute(stream, codec, &portal).await?;
            }
            // Extended messages while in the failed state are skipped until Sync.
            ClientMessage::Parse { .. }
            | ClientMessage::Bind { .. }
            | ClientMessage::Describe { .. }
            | ClientMessage::Close { .. }
            | ClientMessage::Execute { .. } => {}
            ClientMessage::Startup {
                user,
                database,
                application_name,
            } => {
                let session_user = user.clone();
                let session_database = database.clone().unwrap_or_else(|| session_user.clone());
                let startup_application_name = application_name.clone().unwrap_or_default();
                let mut replies = self.state.handle_message(ClientMessage::Startup {
                    user,
                    database,
                    application_name,
                })?;
                self.session_gucs = Arc::new(SessionGucs::new(startup_application_name));
                self.reported_application_name = self.session_gucs.application_name();
                // Register a cancellation key for this connection and announce it
                // with BackendKeyData, placed after the ParameterStatus messages
                // and before the trailing ReadyForQuery.
                let key = self
                    .app
                    .components
                    .cancel_registry
                    .register(self.cancel.clone());
                self.backend_key = Some(key);
                self.session_info = Arc::new(SessionInfo {
                    user: session_user,
                    database: session_database,
                    backend_pid: key.process_id,
                });
                if let Some(activity) = self.activity.take() {
                    self.app.components.session_registry.deregister(&activity);
                }
                self.activity = Some(
                    self.app
                        .components
                        .session_registry
                        .register(self.session_info.clone(), self.session_gucs.clone()),
                );
                let insert_at = replies.len().saturating_sub(1);
                replies.insert(
                    insert_at,
                    ServerMessage::BackendKeyData {
                        process_id: key.process_id,
                        secret_key: key.secret_key,
                    },
                );
                write_messages(stream, codec, &replies).await?;
            }
            other => {
                for response in self.state.handle_message(other)? {
                    write_messages(stream, codec, &[response]).await?;
                }
                if self.state.is_terminated() {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}

/// Resolve a PostgreSQL format-code array (`0` codes = all text, `1` code =
/// applies to every item, `n` codes = per item) to the code for one position.
fn resolve_format(formats: &[i16], index: usize) -> i16 {
    match formats {
        [] => 0,
        [single] => *single,
        many => many.get(index).copied().unwrap_or(0),
    }
}

fn resolve_result_formats(formats: &[i16], columns: &[ColumnInfo]) -> Vec<i16> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let wire_type = column.wire_type();
            resolve_result_format_for_type(formats, index, &wire_type)
        })
        .collect()
}

fn resolve_result_format_for_type(formats: &[i16], index: usize, wire_type: &PgType) -> i16 {
    let requested = resolve_format(formats, index);
    if requested == 1 && binary_result_output_uses_text(wire_type) {
        0
    } else {
        requested
    }
}

fn binary_result_output_uses_text(wire_type: &PgType) -> bool {
    matches!(
        wire_type,
        PgType::OidVector | PgType::Int2Vector | PgType::OidArray | PgType::Int2Array
    )
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::protocol(SqlState::SyntaxError, message)
}

/// Resolve a streamed query `spawn_blocking` task's join result, shared by the
/// simple- and extended-query paths. A panic in parse/bind/plan/execute (or a
/// cancelled task) surfaces as a `JoinError`; mapping it to an internal error
/// with no open transaction (`slot = None`, the default isolation unchanged) lets
/// the caller report it and keep the connection open instead of dropping the
/// socket silently. The wire codec buffer is unaffected and statement guards /
/// page pins release on unwind. The lost transaction's guard/registry entry
/// cannot be recovered here, so a panicked in-transaction statement is
/// best-effort abandoned (matching the pre-streaming behavior).
fn streamed_task_result(
    join: std::result::Result<
        (Option<Transaction>, IsolationLevel, Result<StreamOutcome>),
        tokio::task::JoinError,
    >,
    fallback_default: IsolationLevel,
) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
    join.unwrap_or_else(|join_err| {
        (
            None,
            fallback_default,
            Err(DbError::internal(format!("query task failed: {join_err}"))),
        )
    })
}

async fn write_messages<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    messages: &[ServerMessage],
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    for message in messages {
        socket
            .write_all(&codec.encode(message))
            .await
            .map_err(|err| DbError::io(format!("failed to write socket response: {err}")))?;
    }
    Ok(())
}

/// Encode each column of a result row to its wire bytes, choosing each column's
/// format from the (text/binary) format-code array (empty = all text). The
/// column list supplies each value's declared wire type, so a narrow integer in
/// binary format is encoded to its advertised 2-/4-byte width.
fn encode_row(row: &Row, columns: &[ColumnInfo], formats: &[i16]) -> Result<Vec<Option<Vec<u8>>>> {
    row.values
        .iter()
        .enumerate()
        .map(|(index, value)| match columns.get(index) {
            Some(column) => {
                let wire_type = column.wire_type();
                let format = resolve_result_format_for_type(formats, index, &wire_type);
                protocol::encode_value_with_type(value, &wire_type, format)
            }
            None => protocol::encode_value(value, resolve_format(formats, index)),
        })
        .collect()
}

fn command_complete_tag(command: &str, count: u64) -> String {
    match command {
        "INSERT" => format!("INSERT 0 {count}"),
        "UPDATE" | "DELETE" => format!("{command} {count}"),
        _ => command.to_string(),
    }
}

fn error_response(err: &DbError) -> ServerMessage {
    ServerMessage::ErrorResponse {
        severity: "ERROR".to_string(),
        code: err.code.code().to_string(),
        message: err.message.clone(),
    }
}

#[cfg(test)]
fn sqlstate_code(code: SqlState) -> &'static str {
    code.code()
}

#[cfg(test)]
mod tests;
