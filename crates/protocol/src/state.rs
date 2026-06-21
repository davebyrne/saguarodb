use common::{DbError, Result};

use crate::{ClientMessage, ServerMessage};

pub trait ConnectionState: Send {
    fn handle_message(&mut self, msg: ClientMessage) -> Result<Vec<ServerMessage>>;
    fn is_terminated(&self) -> bool;
}

pub struct PostgresConnectionState {
    terminated: bool,
}

impl PostgresConnectionState {
    pub fn new() -> Self {
        Self { terminated: false }
    }
}

impl Default for PostgresConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionState for PostgresConnectionState {
    fn handle_message(&mut self, msg: ClientMessage) -> Result<Vec<ServerMessage>> {
        match msg {
            // GSSAPI transport encryption is unsupported; decline with the same
            // single `N` byte as SSL rejection. The leading-request case is
            // handled by the server's negotiation loop; this covers a stray
            // request seen mid-stream.
            ClientMessage::SslRequest | ClientMessage::GssEncRequest => {
                Ok(vec![ServerMessage::SslRejected])
            }
            ClientMessage::Startup {
                application_name, ..
            } => Ok(vec![
                ServerMessage::AuthenticationOk,
                ServerMessage::ParameterStatus {
                    key: "server_version".to_string(),
                    value: "16.0".to_string(),
                },
                ServerMessage::ParameterStatus {
                    key: "server_encoding".to_string(),
                    value: "UTF8".to_string(),
                },
                ServerMessage::ParameterStatus {
                    key: "client_encoding".to_string(),
                    value: "UTF8".to_string(),
                },
                ServerMessage::ParameterStatus {
                    key: "DateStyle".to_string(),
                    value: "ISO".to_string(),
                },
                ServerMessage::ParameterStatus {
                    key: "integer_datetimes".to_string(),
                    value: "on".to_string(),
                },
                ServerMessage::ParameterStatus {
                    key: "standard_conforming_strings".to_string(),
                    value: "on".to_string(),
                },
                ServerMessage::ParameterStatus {
                    key: "TimeZone".to_string(),
                    value: "UTC".to_string(),
                },
                // Echo the client's application_name (empty when not supplied),
                // mirroring PostgreSQL's startup reporting.
                ServerMessage::ParameterStatus {
                    key: "application_name".to_string(),
                    value: application_name.unwrap_or_default(),
                },
                ServerMessage::ReadyForQuery,
            ]),
            ClientMessage::Query(_) => Ok(Vec::new()),
            // Extended-query-protocol messages need socket and query-service
            // access, so the server dispatches them directly (like Query) rather
            // than through this state machine. Reaching here is a routing bug.
            ClientMessage::Parse { .. }
            | ClientMessage::Bind { .. }
            | ClientMessage::Describe { .. }
            | ClientMessage::Execute { .. }
            | ClientMessage::Close { .. }
            | ClientMessage::Sync
            | ClientMessage::Flush => Err(DbError::internal(
                "extended query protocol messages must be handled by the server, \
                 not the connection state machine",
            )),
            // CancelRequest opens its own connection and is handled by the server
            // during startup (look up the backend key, signal, close). Reaching
            // the state machine is a routing bug.
            ClientMessage::CancelRequest { .. } => Err(DbError::internal(
                "CancelRequest must be handled by the server during connection startup",
            )),
            ClientMessage::Terminate => {
                self.terminated = true;
                Ok(Vec::new())
            }
        }
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
