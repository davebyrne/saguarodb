use common::Result;

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
            ClientMessage::SslRequest => Ok(vec![ServerMessage::SslRejected]),
            ClientMessage::Startup { .. } => Ok(vec![
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
                ServerMessage::ReadyForQuery,
            ]),
            ClientMessage::Query(_) => Ok(Vec::new()),
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
