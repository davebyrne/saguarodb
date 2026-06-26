use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{ColumnInfo, DataType, DbError, IsolationLevel, Result, Row, SqlState, Value};
use executor::{CopyJob, ExecutionResult};
use protocol::{
    ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
    ServerMessage, StatementKind,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::app::AppState;
use crate::cancel::BackendKey;
use crate::query::{
    CopyInChunk, PreparedStatement, SessionTxnStatus, Transaction, abort_session_transaction,
};
use crate::shutdown::InFlightQueryGuard;

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
    /// explicit `ISOLATION LEVEL` inherits it, while `SET SESSION CHARACTERISTICS`
    /// updates it. It persists across transactions on this connection and resets to
    /// `ReadCommitted` for each new connection (this field is per-`Session`).
    default_isolation: IsolationLevel,
    /// Shared with the running query's `ExecutionContext`; set from another
    /// connection's `CancelRequest` to abort the in-flight query.
    cancel: Arc<AtomicBool>,
    /// This connection's cancellation key, registered at startup and removed on
    /// disconnect.
    backend_key: Option<BackendKey>,
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
            cancel: Arc::new(AtomicBool::new(false)),
            backend_key: None,
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
                let status = self.status_byte();
                write_messages(stream, codec, &[ServerMessage::ReadyForQuery(status)]).await?;
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
            msg @ ClientMessage::Startup { .. } => {
                let mut replies = self.state.handle_message(msg)?;
                // Register a cancellation key for this connection and announce it
                // with BackendKeyData, placed after the ParameterStatus messages
                // and before the trailing ReadyForQuery.
                let key = self
                    .app
                    .components
                    .cancel_registry
                    .register(self.cancel.clone());
                self.backend_key = Some(key);
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

    async fn run_query<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: String,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        // A simple query clears any aborted extended-query sequence, matching
        // PostgreSQL. The transaction-block status (`self.tx`) is owned by the
        // explicit transaction lifecycle and is updated from the slot returned
        // below, not reset here.
        self.failed = false;
        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                // Reject due to shutdown: report the current (pre-statement)
                // status and close. The open transaction (if any) is aborted when
                // the session drops.
                write_messages(
                    stream,
                    codec,
                    &[
                        error_response(&err),
                        ServerMessage::ReadyForQuery(self.status_byte()),
                    ],
                )
                .await?;
                return Ok(ControlFlow::Break(()));
            }
        };
        let service = self.app.query_service.clone();
        let cancel = self.begin_cancelable();
        // Move the session's transaction slot AND default isolation into the blocking
        // task so the whole statement (including any owned write guard) runs on one
        // thread, then take them both back along with the result. The default is
        // threaded in/out like the slot so `SET SESSION CHARACTERISTICS` persists it
        // and a new `BEGIN` inherits it (`docs/specs/mvcc.md` §10 Milestone G2).
        let txn = self.txn.take();
        let default_isolation = self.default_isolation;
        let task = tokio::task::spawn_blocking(move || {
            service.execute_simple(&sql, txn, default_isolation, &cancel)
        })
        .await;
        // `guard` (the in-flight-query guard) is dropped per result arm below: the
        // normal arms drop it before writing the response (the query work is done);
        // the COPY arms hand it to the streaming driver so the COPY keeps counting
        // as in-flight for its whole lifetime (graceful-shutdown coordination).
        let result = match task {
            Ok((txn, default_isolation, result)) => {
                self.txn = txn;
                self.default_isolation = default_isolation;
                result
            }
            Err(join_err) => {
                // The blocking task panicked and lost the transaction slot. Treat
                // the connection as having no open transaction (the panic firewall
                // surfaces an internal error); the guard/registry entry for a lost
                // txn cannot be recovered here, so this is best-effort. The session
                // default is left as-is (it never moved into a committed effect).
                self.txn = None;
                Err(DbError::internal(format!("query task failed: {join_err}")))
            }
        };
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();
        match result {
            // COPY enters its sub-protocol instead of returning a finished result:
            // `BeginCopyIn` spawns the streaming insert and routes subsequent
            // CopyData; `BeginCopyOut` streams the table out inline. Both recompute
            // the transaction status themselves, so the `status` above is unused here.
            Ok(ExecutionResult::BeginCopyIn(job)) => {
                self.begin_copy_in(stream, codec, job, guard).await?
            }
            Ok(ExecutionResult::BeginCopyOut(job)) => {
                self.run_copy_out(stream, codec, job, guard).await?
            }
            Ok(result) => {
                drop(guard);
                write_execution_result(stream, codec, result, status).await?
            }
            Err(err) => {
                drop(guard);
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await?
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Begin `COPY ... FROM STDIN`: send `CopyInResponse`, spawn the blocking
    /// insert task (which owns the transaction, moved out of the session), and
    /// record the copy-in state so subsequent `CopyData` is routed to it. Returns
    /// without waiting — finalization happens on `CopyDone`/`CopyFail`.
    async fn begin_copy_in<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        job: CopyJob,
        guard: InFlightQueryGuard,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let column_formats = vec![0i16; job.columns.len()];
        write_messages(
            stream,
            codec,
            &[ServerMessage::CopyInResponse {
                overall_format: 0,
                column_formats,
            }],
        )
        .await?;

        // A bounded channel gives TCP backpressure: when the insert task lags, the
        // forwarder's `send` awaits and the socket read stalls.
        let (sender, receiver) = mpsc::channel::<CopyInChunk>(64);
        let service = self.app.query_service.clone();
        let txn = self.txn.take();
        let cancel = self.begin_cancelable();
        let task = tokio::task::spawn_blocking(move || {
            service.run_copy_in_stream(job, txn, &cancel, receiver)
        });
        self.copy_in = Some(CopyInSession {
            sender,
            task,
            insert_failed: false,
            _guard: guard,
        });
        Ok(())
    }

    /// Forward one `CopyData` payload to the insert task. If the task has exited
    /// early (a row failed), discard further data until the terminator.
    async fn handle_copy_data(&mut self, bytes: Vec<u8>) -> Result<()> {
        let Some(copy) = self.copy_in.as_mut() else {
            return Err(protocol_error(
                "CopyData received outside of an active COPY",
            ));
        };
        if !copy.insert_failed && copy.sender.send(CopyInChunk::Chunk(bytes)).await.is_err() {
            // The receiver was dropped because the insert task exited on a row error.
            copy.insert_failed = true;
        }
        Ok(())
    }

    /// Finalize a `COPY ... FROM STDIN` on `CopyDone` (`fail_message` `None`) or
    /// `CopyFail` (`Some(message)`): signal the task, await it, restore the session
    /// transaction, and reply. On any failure the inbound stream has already been
    /// drained to the terminator, so `ReadyForQuery` is emitted last.
    async fn finish_copy_in<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        fail_message: Option<String>,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let copy = self
            .copy_in
            .take()
            .expect("finish_copy_in called with no active COPY");
        let insert_failed = copy.insert_failed;
        if !insert_failed {
            // Signal a clean end (`Done` → commit) or a client abort (`Fail`).
            let signal = if fail_message.is_some() {
                CopyInChunk::Fail
            } else {
                CopyInChunk::Done
            };
            let _ = copy.sender.send(signal).await;
        }
        drop(copy.sender);
        let (txn, result) = match copy.task.await {
            Ok(pair) => pair,
            Err(join_err) => (
                None,
                Err(DbError::internal(format!("COPY task failed: {join_err}"))),
            ),
        };
        self.txn = txn;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();

        match result {
            Ok(count) => {
                write_messages(
                    stream,
                    codec,
                    &[
                        ServerMessage::CommandComplete(format!("COPY {count}")),
                        ServerMessage::ReadyForQuery(status),
                    ],
                )
                .await
            }
            Err(task_err) => {
                // A client CopyFail (with no prior insert error) reports the client's
                // message; otherwise the insert/row error.
                let err = match fail_message {
                    Some(message) if !insert_failed => DbError::execute(
                        SqlState::QueryCanceled,
                        format!("COPY from stdin failed: {message}"),
                    ),
                    _ => task_err,
                };
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await
            }
        }
    }

    /// Run `COPY ... TO STDOUT` inline: send `CopyOutResponse`, stream rendered
    /// frames from the blocking producer to the socket, then `CopyDone` +
    /// `CommandComplete` (or `ErrorResponse` on failure, with no `CopyDone`).
    async fn run_copy_out<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        job: CopyJob,
        // Held for the COPY's lifetime so it counts as an in-flight query during the
        // streaming scan; dropped when this returns.
        _guard: InFlightQueryGuard,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let column_formats = vec![0i16; job.columns.len()];
        write_messages(
            stream,
            codec,
            &[ServerMessage::CopyOutResponse {
                overall_format: 0,
                column_formats,
            }],
        )
        .await?;

        let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(8);
        let service = self.app.query_service.clone();
        let txn = self.txn.take();
        let cancel = self.begin_cancelable();
        let task = tokio::task::spawn_blocking(move || {
            service.run_copy_out_stream(job, txn, &cancel, frame_tx)
        });

        let mut write_err = None;
        while let Some(frame) = frame_rx.recv().await {
            if let Err(err) = write_messages(stream, codec, &[ServerMessage::CopyData(frame)]).await
            {
                write_err = Some(err);
                break;
            }
        }
        // Drop the receiver so the producer's next `blocking_send` fails fast if we
        // broke out early on a socket error.
        drop(frame_rx);

        let (txn, result) = match task.await {
            Ok(pair) => pair,
            Err(join_err) => (
                None,
                Err(DbError::internal(format!("COPY task failed: {join_err}"))),
            ),
        };
        self.txn = txn;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();

        if let Some(err) = write_err {
            return Err(err);
        }
        match result {
            Ok(count) => {
                write_messages(
                    stream,
                    codec,
                    &[
                        ServerMessage::CopyDone,
                        ServerMessage::CommandComplete(format!("COPY {count}")),
                        ServerMessage::ReadyForQuery(status),
                    ],
                )
                .await
            }
            // A producer error after CopyOutResponse: ErrorResponse, no CopyDone.
            Err(err) => {
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await
            }
        }
    }

    async fn run_execute<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        portal_name: &str,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let Some(portal) = self.portals.get(portal_name) else {
            self.failed = true;
            let err = protocol_error(format!("portal \"{portal_name}\" does not exist"));
            return write_messages(stream, codec, &[error_response(&err)]).await;
        };
        let statement = portal.statement.clone();
        let params = portal.params.clone();
        let result_formats = portal.result_formats.clone();

        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                self.failed = true;
                return write_messages(stream, codec, &[error_response(&err)]).await;
            }
        };
        let service = self.app.query_service.clone();
        let cancel = self.begin_cancelable();

        // When an explicit transaction is open on this session, the extended-
        // protocol `Execute` participates in THAT transaction rather than starting
        // an independent autocommit unit. Routing both protocols through the one
        // transaction slot guarantees the session acquires the exclusive write
        // guard at most once: an open write transaction holds its single guard, and
        // an in-transaction `Execute` reuses it (or lazily acquires it once on the
        // first write) instead of re-acquiring it and self-deadlocking. A
        // transaction-control `Execute` (BEGIN/COMMIT/ROLLBACK) is also routed
        // through the session path even with no transaction open, so it drives
        // `self.txn` like a simple-query control statement. Otherwise (a data
        // statement with no open transaction), `Execute` stays a self-contained
        // autocommit unit.
        let route_through_session =
            self.txn.is_some() || statement.is_transaction_control() || statement.is_maintenance();
        let result = if route_through_session {
            // Move the transaction slot AND default isolation into the blocking task
            // (like the simple-query path) so the whole statement, including any owned
            // write guard, runs on one thread; take them both back with the result. A
            // transaction-control `Execute` (e.g. BEGIN, or SET SESSION CHARACTERISTICS
            // routed via the session path) reads/updates the default here.
            let txn = self.txn.take();
            let default_isolation = self.default_isolation;
            let task = tokio::task::spawn_blocking(move || {
                service.execute_prepared_in_session(
                    &statement,
                    &params,
                    txn,
                    default_isolation,
                    &cancel,
                )
            })
            .await;
            drop(guard);
            match task {
                Ok((txn, default_isolation, result)) => {
                    self.txn = txn;
                    self.default_isolation = default_isolation;
                    result
                }
                Err(join_err) => {
                    // The blocking task panicked and lost the transaction slot; the
                    // guard/registry entry for the lost txn cannot be recovered
                    // here. Treat the session as having no open transaction (the
                    // simple-query path makes the same best-effort choice).
                    self.txn = None;
                    Err(DbError::internal(format!("query task failed: {join_err}")))
                }
            }
        } else {
            let result = query_task_result(
                tokio::task::spawn_blocking(move || {
                    service.execute_prepared_cancelable(&statement, &params, &cancel)
                })
                .await,
            );
            drop(guard);
            result
        };

        // Keep the reported transaction-block status in sync with the slot, so the
        // `ReadyForQuery` that `Sync` later emits carries the right `I`/`T`/`E` byte.
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));

        match result {
            Ok(result) => write_portal_result(stream, codec, result, &result_formats).await,
            Err(err) => {
                self.failed = true;
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }

    fn process_parse(
        &mut self,
        name: String,
        query: String,
        param_type_oids: &[i32],
    ) -> Result<Vec<ServerMessage>> {
        let declared = param_type_oids
            .iter()
            .map(|oid| oid_to_data_type(*oid))
            .collect::<Result<Vec<_>>>()?;
        let prepared = self.app.query_service.prepare_sql(&query, &declared)?;
        self.prepared.insert(name, Arc::new(prepared));
        Ok(vec![ServerMessage::ParseComplete])
    }

    fn process_bind(
        &mut self,
        portal: String,
        statement: &str,
        param_formats: &[i16],
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<Vec<ServerMessage>> {
        let prepared = self.prepared.get(statement).cloned().ok_or_else(|| {
            protocol_error(format!("prepared statement \"{statement}\" does not exist"))
        })?;
        let params = decode_bind_params(&prepared, param_formats, &params)?;
        self.portals.insert(
            portal,
            Portal {
                statement: prepared,
                params,
                result_formats,
            },
        );
        Ok(vec![ServerMessage::BindComplete])
    }

    fn process_describe(&self, kind: StatementKind, name: &str) -> Result<Vec<ServerMessage>> {
        match kind {
            StatementKind::Statement => {
                let prepared = self.prepared.get(name).ok_or_else(|| {
                    protocol_error(format!("prepared statement \"{name}\" does not exist"))
                })?;
                let oids = prepared
                    .param_types()
                    .iter()
                    .map(protocol::type_oid)
                    .collect();
                Ok(vec![
                    ServerMessage::ParameterDescription(oids),
                    row_description_or_no_data(prepared.result_columns(), &[]),
                ])
            }
            StatementKind::Portal => {
                let portal = self
                    .portals
                    .get(name)
                    .ok_or_else(|| protocol_error(format!("portal \"{name}\" does not exist")))?;
                Ok(vec![row_description_or_no_data(
                    portal.statement.result_columns(),
                    &portal.result_formats,
                )])
            }
        }
    }

    fn process_close(&mut self, kind: StatementKind, name: &str) -> Result<Vec<ServerMessage>> {
        match kind {
            StatementKind::Statement => {
                self.prepared.remove(name);
            }
            StatementKind::Portal => {
                self.portals.remove(name);
            }
        }
        Ok(vec![ServerMessage::CloseComplete])
    }

    async fn reply_or_fail<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        result: Result<Vec<ServerMessage>>,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        match result {
            Ok(messages) => write_messages(stream, codec, &messages).await,
            Err(err) => {
                self.failed = true;
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }
}

fn oid_to_data_type(oid: i32) -> Result<Option<DataType>> {
    match oid {
        0 => Ok(None),
        20 => Ok(Some(DataType::Integer)),
        25 => Ok(Some(DataType::Text)),
        16 => Ok(Some(DataType::Boolean)),
        other => Err(protocol_error(format!(
            "unsupported parameter type OID {other}"
        ))),
    }
}

fn decode_bind_params(
    prepared: &PreparedStatement,
    param_formats: &[i16],
    params: &[Option<Vec<u8>>],
) -> Result<Vec<Value>> {
    let types = prepared.param_types();
    if params.len() != types.len() {
        return Err(protocol_error(format!(
            "bind message supplies {} parameter value(s), but the statement requires {}",
            params.len(),
            types.len()
        )));
    }
    params
        .iter()
        .enumerate()
        .map(|(index, raw)| match raw {
            None => Ok(Value::Null),
            Some(bytes) => protocol::decode_value(
                bytes,
                types[index].clone(),
                resolve_format(param_formats, index),
            ),
        })
        .collect()
}

fn row_description_or_no_data(columns: Option<&[ColumnInfo]>, formats: &[i16]) -> ServerMessage {
    match columns {
        Some(columns) => ServerMessage::RowDescription {
            formats: resolve_formats(formats, columns.len()),
            columns: columns.to_vec(),
        },
        None => ServerMessage::NoData,
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

fn resolve_formats(formats: &[i16], count: usize) -> Vec<i16> {
    (0..count)
        .map(|index| resolve_format(formats, index))
        .collect()
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::protocol(SqlState::SyntaxError, message)
}

/// Map the outcome of the query `spawn_blocking` task into a query result. A
/// panic in parse/bind/plan/execute (or a cancelled task) surfaces as a
/// `JoinError`; converting it to an internal error lets the caller report it and
/// keep the connection open instead of dropping the socket silently. The wire
/// codec buffer is unaffected and statement guards/page pins release on unwind.
fn query_task_result(
    join: std::result::Result<Result<ExecutionResult>, tokio::task::JoinError>,
) -> Result<ExecutionResult> {
    join.unwrap_or_else(|join_err| Err(DbError::internal(format!("query task failed: {join_err}"))))
}

/// Write the result of a simple query, terminated by a `ReadyForQuery` carrying
/// the session's current transaction-status `status` byte.
async fn write_execution_result<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    result: ExecutionResult,
    status: u8,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match result {
        ExecutionResult::Query { columns, rows } => {
            let mut messages = Vec::with_capacity(rows.len() + 3);
            messages.push(ServerMessage::RowDescription {
                columns,
                formats: Vec::new(),
            });
            for row in rows {
                messages.push(ServerMessage::DataRow(encode_row(&row, &[])?));
            }
            let count = messages.len().saturating_sub(1);
            messages.push(ServerMessage::CommandComplete(format!("SELECT {count}")));
            messages.push(ServerMessage::ReadyForQuery(status));
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Modified { command, count } => {
            let tag = command_complete_tag(&command, count);
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::CommandComplete(tag),
                    ServerMessage::ReadyForQuery(status),
                ],
            )
            .await
        }
        ExecutionResult::Explanation { text } => {
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::RowDescription {
                        columns: vec![ColumnInfo {
                            name: "QUERY PLAN".to_string(),
                            data_type: DataType::Text,
                            table_id: None,
                            column_id: None,
                        }],
                        formats: Vec::new(),
                    },
                    ServerMessage::DataRow(vec![Some(text.into_bytes())]),
                    ServerMessage::CommandComplete("EXPLAIN".to_string()),
                    ServerMessage::ReadyForQuery(status),
                ],
            )
            .await
        }
        // COPY requests are intercepted by `run_query` and driven by the COPY
        // sub-protocol; they never reach the generic result writer.
        ExecutionResult::BeginCopyIn(_) | ExecutionResult::BeginCopyOut(_) => Err(
            DbError::internal("COPY result must be handled by the connection loop"),
        ),
    }
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

/// Encode each column of a result row to its wire bytes in the given format
/// code (`0` = text, `1` = binary), or `None` for SQL NULL.
/// Encode each column of a result row to its wire bytes, choosing each column's
/// format from the (text/binary) format-code array (empty = all text).
fn encode_row(row: &Row, formats: &[i16]) -> Result<Vec<Option<Vec<u8>>>> {
    row.values
        .iter()
        .enumerate()
        .map(|(index, value)| protocol::encode_value(value, resolve_format(formats, index)))
        .collect()
}

/// Write the result of an extended-protocol `Execute`: data rows (in the
/// portal's result formats) and `CommandComplete`. Unlike the simple-query path
/// it sends no `RowDescription` (that comes from `Describe`) and no
/// `ReadyForQuery` (that comes from `Sync`).
async fn write_portal_result<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    result: ExecutionResult,
    result_formats: &[i16],
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match result {
        ExecutionResult::Query { rows, .. } => {
            let mut messages = Vec::with_capacity(rows.len() + 1);
            for row in &rows {
                messages.push(ServerMessage::DataRow(encode_row(row, result_formats)?));
            }
            messages.push(ServerMessage::CommandComplete(format!(
                "SELECT {}",
                rows.len()
            )));
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Modified { command, count } => {
            write_messages(
                socket,
                codec,
                &[ServerMessage::CommandComplete(command_complete_tag(
                    &command, count,
                ))],
            )
            .await
        }
        ExecutionResult::Explanation { text } => {
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::DataRow(vec![Some(text.into_bytes())]),
                    ServerMessage::CommandComplete("EXPLAIN".to_string()),
                ],
            )
            .await
        }
        // COPY is rejected in the extended query protocol before execution, so a
        // portal never yields a COPY request.
        ExecutionResult::BeginCopyIn(_) | ExecutionResult::BeginCopyOut(_) => Err(
            DbError::internal("COPY is not valid in the extended query protocol"),
        ),
    }
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
        code: sqlstate_code(err.code).to_string(),
        message: err.message.clone(),
    }
}

fn sqlstate_code(code: SqlState) -> &'static str {
    match code {
        SqlState::SuccessfulCompletion => "00000",
        SqlState::SyntaxError => "42601",
        SqlState::UndefinedTable => "42P01",
        SqlState::UndefinedColumn => "42703",
        SqlState::InvalidColumnReference => "42P10",
        SqlState::DuplicateTable => "42P07",
        SqlState::DatatypeMismatch => "42804",
        SqlState::DivisionByZero => "22012",
        SqlState::NumericValueOutOfRange => "22003",
        SqlState::InvalidTextRepresentation => "22P02",
        SqlState::BadCopyFileFormat => "22P04",
        SqlState::NotNullViolation => "23502",
        SqlState::UniqueViolation => "23505",
        SqlState::QueryCanceled => "57014",
        SqlState::FeatureNotSupported => "0A000",
        SqlState::InFailedSqlTransaction => "25P02",
        SqlState::SerializationFailure => "40001",
        SqlState::IoError => "58030",
        SqlState::InternalError => "XX000",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use common::{ErrorKind, Result};
    use executor::ExecutionResult;

    use super::{TransactionState, handle_connection, query_task_result};
    use crate::app::AppState;

    #[test]
    fn transaction_state_maps_to_postgres_status_byte() {
        assert_eq!(TransactionState::Idle.status_byte(), b'I');
        assert_eq!(TransactionState::InTransaction.status_byte(), b'T');
        assert_eq!(TransactionState::Failed.status_byte(), b'E');
    }

    #[tokio::test]
    async fn panicked_query_task_becomes_internal_error() {
        // A panicked spawn_blocking task yields a real JoinError; the firewall
        // must map it to an internal error (so the caller keeps the connection
        // open) rather than letting it escape and drop the connection.
        let join = tokio::task::spawn_blocking(|| -> Result<ExecutionResult> {
            panic!("intentional test panic");
        })
        .await;

        let err = query_task_result(join).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Internal);
    }

    #[tokio::test]
    async fn loopback_startup_and_simple_query_return_protocol_rows() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client
            .write_all(&query_bytes(
                "create table users (id integer primary key, name text)",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes(
                "insert into users (id, name) values (1, 'Ada')",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes("select id, name from users"))
            .await
            .unwrap();
        let response = read_until_ready(&mut client).await;

        assert!(response.windows(3).any(|window| window == b"Ada"));
        assert!(response.windows(9).any(|window| window == b"SELECT 1\0"));

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_characteristics_default_is_per_connection_over_the_wire() {
        // End-to-end (Milestone G2): `SET SESSION CHARACTERISTICS ... REPEATABLE READ`
        // on one connection makes a later plain `BEGIN` on THAT connection default to
        // Repeatable Read (its second SELECT does not see a row committed in between).
        // A fresh connection resets to Read Committed and DOES see such a row.
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept connections for the duration of the test on a background task.
        let accept_app = app.clone();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let app = accept_app.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(socket, app).await;
                });
            }
        });

        // Setup connection: create the table and seed nothing.
        let mut setup = TcpStream::connect(addr).await.unwrap();
        setup.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut setup).await;
        setup
            .write_all(&query_bytes("create table t (id integer primary key)"))
            .await
            .unwrap();
        read_until_ready(&mut setup).await;

        // Connection A: set the session default to Repeatable Read, then open a txn.
        let mut conn_a = TcpStream::connect(addr).await.unwrap();
        conn_a.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut conn_a).await;
        conn_a
            .write_all(&query_bytes(
                "set session characteristics as transaction isolation level repeatable read",
            ))
            .await
            .unwrap();
        let response = read_until_ready(&mut conn_a).await;
        assert!(
            response.windows(4).any(|w| w == b"SET\0"),
            "SET SESSION CHARACTERISTICS completes with a SET tag"
        );

        conn_a.write_all(&query_bytes("begin")).await.unwrap();
        read_until_ready_any(&mut conn_a).await;
        conn_a
            .write_all(&query_bytes("select id from t"))
            .await
            .unwrap();
        let response = read_until_ready_any(&mut conn_a).await;
        assert!(response.windows(9).any(|w| w == b"SELECT 0\0"));

        // The setup connection commits a row while conn A's RR txn is open.
        setup
            .write_all(&query_bytes("insert into t (id) values (1)"))
            .await
            .unwrap();
        read_until_ready(&mut setup).await;

        // Conn A's second SELECT does NOT see the new row (it defaulted to RR).
        conn_a
            .write_all(&query_bytes("select id from t"))
            .await
            .unwrap();
        let response = read_until_ready_any(&mut conn_a).await;
        assert!(
            response.windows(9).any(|w| w == b"SELECT 0\0"),
            "the inherited RR transaction does not see the concurrently-committed row"
        );
        conn_a.write_all(&query_bytes("commit")).await.unwrap();
        read_until_ready(&mut conn_a).await;
        conn_a.write_all(&terminate_bytes()).await.unwrap();

        // Connection B (fresh): resets to Read Committed regardless of conn A's
        // setting. Its open transaction's second SELECT DOES see a new committed row.
        let mut conn_b = TcpStream::connect(addr).await.unwrap();
        conn_b.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut conn_b).await;
        conn_b.write_all(&query_bytes("begin")).await.unwrap();
        read_until_ready_any(&mut conn_b).await;
        conn_b
            .write_all(&query_bytes("select id from t"))
            .await
            .unwrap();
        let response = read_until_ready_any(&mut conn_b).await;
        assert!(response.windows(9).any(|w| w == b"SELECT 1\0"));
        setup
            .write_all(&query_bytes("insert into t (id) values (2)"))
            .await
            .unwrap();
        read_until_ready(&mut setup).await;
        conn_b
            .write_all(&query_bytes("select id from t"))
            .await
            .unwrap();
        let response = read_until_ready_any(&mut conn_b).await;
        assert!(
            response.windows(9).any(|w| w == b"SELECT 2\0"),
            "a fresh connection resets to Read Committed and sees the new row"
        );
        conn_b.write_all(&query_bytes("commit")).await.unwrap();
        read_until_ready(&mut conn_b).await;
        conn_b.write_all(&terminate_bytes()).await.unwrap();
        setup.write_all(&terminate_bytes()).await.unwrap();

        server.abort();
    }

    fn startup_bytes(user: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
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

    fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut packet = vec![tag];
        packet.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

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

    fn bind_bytes(
        portal: &str,
        statement: &str,
        param_formats: &[i16],
        params: &[Option<&[u8]>],
        result_formats: &[i16],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(statement.as_bytes());
        body.push(0);
        body.extend_from_slice(&i16::try_from(param_formats.len()).unwrap().to_be_bytes());
        for format in param_formats {
            body.extend_from_slice(&format.to_be_bytes());
        }
        body.extend_from_slice(&i16::try_from(params.len()).unwrap().to_be_bytes());
        for param in params {
            match param {
                Some(bytes) => {
                    body.extend_from_slice(&i32::try_from(bytes.len()).unwrap().to_be_bytes());
                    body.extend_from_slice(bytes);
                }
                None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        body.extend_from_slice(&i16::try_from(result_formats.len()).unwrap().to_be_bytes());
        for format in result_formats {
            body.extend_from_slice(&format.to_be_bytes());
        }
        tagged(b'B', &body)
    }

    fn describe_portal_bytes(name: &str) -> Vec<u8> {
        let mut body = vec![b'P'];
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        tagged(b'D', &body)
    }

    fn describe_statement_bytes(name: &str) -> Vec<u8> {
        let mut body = vec![b'S'];
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        tagged(b'D', &body)
    }

    fn execute_bytes(portal: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        tagged(b'E', &body)
    }

    fn sync_bytes() -> Vec<u8> {
        tagged(b'S', &[])
    }

    #[tokio::test]
    async fn startup_sends_backend_key_data() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        let response = read_until_ready(&mut client).await;

        // BackendKeyData: tag 'K', length 12.
        assert!(
            response.windows(5).any(|w| w == [b'K', 0, 0, 0, 12]),
            "startup reply includes BackendKeyData"
        );

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_request_signals_the_target_backend() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        // Register a flag as if a connection were running a query.
        let flag = Arc::new(AtomicBool::new(false));
        let key = app.components.cancel_registry.register(flag.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_app = app.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, server_app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&cancel_request_bytes(key.process_id, key.secret_key))
            .await
            .unwrap();

        // The server signals the backend and closes without any reply.
        let mut buf = [0u8; 8];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "CancelRequest gets no reply and the socket closes");
        assert!(flag.load(Ordering::Relaxed), "target backend was signaled");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn extended_protocol_runs_parameterized_query_text_and_binary() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (1, 'Ada')",
            "insert into users (id, name) values (2, 'Bo')",
        ] {
            client.write_all(&query_bytes(sql)).await.unwrap();
            read_until_ready(&mut client).await;
        }

        // Text parameter (OID unspecified -> inferred Integer), text results.
        let mut seq = parse_bytes("", "select name from users where id = $1", &[0]);
        seq.extend(bind_bytes("", "", &[0], &[Some(b"1")], &[0]));
        seq.extend(describe_portal_bytes(""));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(5).any(|w| w == [b'1', 0, 0, 0, 4]),
            "ParseComplete"
        );
        assert!(
            response.windows(5).any(|w| w == [b'2', 0, 0, 0, 4]),
            "BindComplete"
        );
        assert!(response.windows(3).any(|w| w == b"Ada"), "row value");
        assert!(
            response.windows(9).any(|w| w == b"SELECT 1\0"),
            "CommandComplete"
        );

        // Binary parameter (int8), binary results.
        let id = 2i64.to_be_bytes();
        let mut seq = parse_bytes("", "select name from users where id = $1", &[20]);
        seq.extend(bind_bytes("", "", &[1], &[Some(&id[..])], &[1]));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(2).any(|w| w == b"Bo"),
            "binary-parameter row value"
        );

        // Binary INTEGER result column: the value is the 8-byte big-endian
        // encoding, distinguishing binary from text result encoding.
        let mut seq = parse_bytes("", "select id from users where id = $1", &[20]);
        seq.extend(bind_bytes("", "", &[1], &[Some(&id[..])], &[1]));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(8).any(|w| w == 2i64.to_be_bytes()),
            "binary int8 result value"
        );

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn extended_protocol_error_is_recoverable_via_sync() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        // Parse referencing a missing table fails; the Execute that follows is
        // skipped, and Sync recovers the connection with ReadyForQuery.
        let mut seq = parse_bytes("", "select id from missing where id = $1", &[0]);
        seq.extend(bind_bytes("", "", &[0], &[Some(b"1")], &[0]));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(5).any(|w| w == b"42P01"),
            "undefined-table error"
        );
        // The Bind/Execute after the failed Parse were skipped until Sync.
        assert!(
            !response.windows(5).any(|w| w == [b'2', 0, 0, 0, 4]),
            "Bind should have been skipped"
        );

        // The connection still works after Sync.
        client
            .write_all(&query_bytes("create table t (id integer primary key)"))
            .await
            .unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(13).any(|w| w == b"CREATE TABLE\0"),
            "connection recovered"
        );

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn extended_protocol_named_statement_and_describe() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (7, 'Cy')",
        ] {
            client.write_all(&query_bytes(sql)).await.unwrap();
            read_until_ready(&mut client).await;
        }

        // Named statement + named portal, with Describe of the statement.
        let mut seq = parse_bytes("stmt1", "select name from users where id = $1", &[20]);
        seq.extend(describe_statement_bytes("stmt1"));
        seq.extend(bind_bytes("p1", "stmt1", &[0], &[Some(b"7")], &[0]));
        seq.extend(execute_bytes("p1"));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;

        // ParameterDescription (tag 't'): one parameter, int8 (OID 20).
        assert!(
            response
                .windows(11)
                .any(|w| w == [b't', 0, 0, 0, 10, 0, 1, 0, 0, 0, 20]),
            "ParameterDescription with int8 parameter"
        );
        assert!(response.first() == Some(&b'1'), "ParseComplete first");
        assert!(response.windows(1).any(|w| w == b"T"), "RowDescription");
        assert!(response.windows(2).any(|w| w == b"Cy"), "row value");

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn loopback_boolean_query_uses_postgres_text_bool_format() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client
            .write_all(&query_bytes(
                "create table users (id integer primary key, active boolean)",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes(
                "insert into users (id, active) values (1, true)",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes("select active from users"))
            .await
            .unwrap();
        let response = read_until_ready(&mut client).await;

        assert!(
            response
                .windows(5)
                .any(|window| window == [0, 0, 0, 1, b't'])
        );
        assert!(!response.windows(4).any(|window| window == b"true"));

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    fn ssl_request_bytes() -> Vec<u8> {
        negotiation_request_bytes(80_877_103)
    }

    fn gssenc_request_bytes() -> Vec<u8> {
        negotiation_request_bytes(80_877_104)
    }

    fn cancel_request_bytes(process_id: i32, secret_key: i32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&16i32.to_be_bytes());
        bytes.extend_from_slice(&80_877_102i32.to_be_bytes());
        bytes.extend_from_slice(&process_id.to_be_bytes());
        bytes.extend_from_slice(&secret_key.to_be_bytes());
        bytes
    }

    fn negotiation_request_bytes(code: i32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&code.to_be_bytes());
        bytes
    }

    /// Generate a self-signed `localhost` cert into `dir`, open a TLS-enabled
    /// test app, and return it with the cert PEM (for the client's trust root).
    fn open_app_with_tls(dir: &std::path::Path) -> (Arc<AppState>, String) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = generated.cert.pem();
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, generated.signing_key.serialize_pem()).unwrap();

        let mut config = crate::recovery::data_dir_for_test(dir);
        config.tls_cert_file = Some(cert_path);
        config.tls_key_file = Some(key_path);
        (
            Arc::new(crate::recovery::open_app(config).unwrap()),
            cert_pem,
        )
    }

    /// Complete a client-side TLS handshake over `tcp`, trusting `cert_pem`.
    async fn connect_tls_client(
        cert_pem: &str,
        tcp: TcpStream,
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::crypto::ring;
        use tokio_rustls::rustls::pki_types::ServerName;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};

        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut cert_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let domain = ServerName::try_from("localhost").unwrap();
        connector.connect(domain, tcp).await.unwrap()
    }

    #[tokio::test]
    async fn ssl_request_is_rejected_when_tls_is_disabled_then_plaintext_proceeds() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&ssl_request_bytes()).await.unwrap();

        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"N");

        // The same connection then completes a plaintext startup.
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_negotiation_upgrades_then_query_runs_over_encrypted_stream() {
        let dir = tempfile::tempdir().unwrap();
        let (app, cert_pem) = open_app_with_tls(dir.path());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&ssl_request_bytes()).await.unwrap();
        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"S");

        let mut tls = connect_tls_client(&cert_pem, client).await;

        tls.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes(
            "create table users (id integer primary key, name text)",
        ))
        .await
        .unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes(
            "insert into users (id, name) values (1, 'Ada')",
        ))
        .await
        .unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes("select id, name from users"))
            .await
            .unwrap();
        let response = read_until_ready(&mut tls).await;

        assert!(response.windows(3).any(|window| window == b"Ada"));
        assert!(response.windows(9).any(|window| window == b"SELECT 1\0"));

        tls.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gssenc_request_is_declined_then_plaintext_startup_proceeds() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&gssenc_request_bytes()).await.unwrap();

        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"N");

        // After declining GSS the same connection completes a plaintext startup.
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gssenc_decline_then_ssl_upgrade_serves_encrypted_session() {
        let dir = tempfile::tempdir().unwrap();
        let (app, cert_pem) = open_app_with_tls(dir.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();

        // Decline GSS, then upgrade via SSL on the same connection.
        client.write_all(&gssenc_request_bytes()).await.unwrap();
        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"N");

        client.write_all(&ssl_request_bytes()).await.unwrap();
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"S");

        // The TLS handshake succeeding here proves the SSLRequest after a GSS
        // decline still reaches the upgrade path rather than being mishandled.
        let mut tls = connect_tls_client(&cert_pem, client).await;
        tls.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes("create table t (id integer primary key)"))
            .await
            .unwrap();
        let response = read_until_ready(&mut tls).await;
        assert!(
            response
                .windows(13)
                .any(|window| window == b"CREATE TABLE\0")
        );

        tls.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    async fn read_until_ready<S: AsyncRead + Unpin>(client: &mut S) -> Vec<u8> {
        let mut response = Vec::new();
        let mut buf = [0; 1024];
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let read = client.read(&mut buf).await.unwrap();
                assert_ne!(read, 0, "connection closed before ReadyForQuery");
                response.extend_from_slice(&buf[..read]);
                if response.windows(6).any(|window| window == b"Z\0\0\0\x05I") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        response
    }

    /// Like [`read_until_ready`] but breaks on a `ReadyForQuery` carrying ANY
    /// transaction-status byte (`I`/`T`/`E`), so it can drain a reply sent while a
    /// transaction block is open (status `'T'`), not just an idle one.
    async fn read_until_ready_any<S: AsyncRead + Unpin>(client: &mut S) -> Vec<u8> {
        let mut response = Vec::new();
        let mut buf = [0; 1024];
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let read = client.read(&mut buf).await.unwrap();
                assert_ne!(read, 0, "connection closed before ReadyForQuery");
                response.extend_from_slice(&buf[..read]);
                let ready = response.windows(6).any(|window| {
                    window[..5] == [b'Z', 0, 0, 0, 5] && matches!(window[5], b'I' | b'T' | b'E')
                });
                if ready {
                    break;
                }
            }
        })
        .await
        .unwrap();
        response
    }
}
