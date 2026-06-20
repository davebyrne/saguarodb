use bytes::BytesMut;
use common::{ColumnInfo, DataType, DbError, Result, SqlState};

use crate::{ClientMessage, ServerMessage};

const SSL_REQUEST_CODE: i32 = 80_877_103;
const POSTGRES_PROTOCOL_V3: i32 = 196_608;
const MAX_FRAME_LEN: usize = 1024 * 1024;

pub trait ProtocolCodec: Send {
    fn decode(&mut self, buf: &[u8]) -> Result<Vec<ClientMessage>>;
    fn encode(&self, msg: &ServerMessage) -> Vec<u8>;
}

pub struct PostgresCodec {
    buffer: BytesMut,
}

impl PostgresCodec {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    fn decode_startup_style(&mut self, messages: &mut Vec<ClientMessage>) -> Result<bool> {
        if self.buffer.len() < 4 {
            return Ok(false);
        }

        let length = read_i32(&self.buffer[..4])?;
        if length < 8 {
            return Err(protocol_error("startup packet length is too short"));
        }
        let length = usize::try_from(length)
            .map_err(|_| protocol_error("startup packet length is invalid"))?;
        if length > MAX_FRAME_LEN {
            return Err(protocol_error(
                "startup packet length exceeds maximum frame size",
            ));
        }
        if self.buffer.len() < length {
            return Ok(false);
        }

        let packet = self.buffer.split_to(length);
        let code = read_i32(&packet[4..8])?;
        if length == 8 && code == SSL_REQUEST_CODE {
            messages.push(ClientMessage::SslRequest);
            return Ok(true);
        }
        if code != POSTGRES_PROTOCOL_V3 {
            return Err(protocol_error("unsupported PostgreSQL startup protocol"));
        }

        let (user, database) = decode_startup_params(&packet[8..])?;
        messages.push(ClientMessage::Startup { user, database });
        Ok(true)
    }

    fn decode_tagged(&mut self, messages: &mut Vec<ClientMessage>) -> Result<bool> {
        if self.buffer.len() < 5 {
            return Ok(false);
        }

        let tag = self.buffer[0];
        let length = read_i32(&self.buffer[1..5])?;
        if length < 4 {
            return Err(protocol_error("tagged message length is too short"));
        }
        let length = usize::try_from(length)
            .map_err(|_| protocol_error("tagged message length is invalid"))?;
        if length > MAX_FRAME_LEN {
            return Err(protocol_error(
                "tagged message length exceeds maximum frame size",
            ));
        }
        let total_length = length
            .checked_add(1)
            .ok_or_else(|| protocol_error("tagged message length overflows"))?;
        if self.buffer.len() < total_length {
            return Ok(false);
        }

        let packet = self.buffer.split_to(total_length);
        let body = &packet[5..];
        match tag {
            b'Q' => {
                let sql = decode_nul_terminated_text(body, "query message is not nul terminated")?;
                messages.push(ClientMessage::Query(sql.to_string()));
            }
            b'X' => {
                if length != 4 {
                    return Err(protocol_error("terminate message has invalid length"));
                }
                messages.push(ClientMessage::Terminate);
            }
            _ => return Err(protocol_error("unsupported frontend message tag")),
        }

        Ok(true)
    }
}

impl Default for PostgresCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolCodec for PostgresCodec {
    fn decode(&mut self, buf: &[u8]) -> Result<Vec<ClientMessage>> {
        self.buffer.extend_from_slice(buf);
        let mut messages = Vec::new();

        loop {
            if self.buffer.is_empty() {
                break;
            }

            let decoded = match self.buffer[0] {
                b'Q' | b'X' => self.decode_tagged(&mut messages)?,
                0 => self.decode_startup_style(&mut messages)?,
                _ => return Err(protocol_error("unsupported frontend message tag")),
            };
            if !decoded {
                break;
            }
        }

        Ok(messages)
    }

    fn encode(&self, msg: &ServerMessage) -> Vec<u8> {
        match msg {
            ServerMessage::SslAccepted => b"S".to_vec(),
            ServerMessage::SslRejected => b"N".to_vec(),
            ServerMessage::AuthenticationOk => {
                let mut body = Vec::new();
                put_i32(&mut body, 0);
                encode_server_message(b'R', body)
            }
            ServerMessage::ParameterStatus { key, value } => {
                let mut body = Vec::new();
                put_cstr(&mut body, key);
                put_cstr(&mut body, value);
                encode_server_message(b'S', body)
            }
            ServerMessage::ReadyForQuery => encode_server_message(b'Z', vec![b'I']),
            ServerMessage::RowDescription(columns) => {
                let mut body = Vec::new();
                put_i16(
                    &mut body,
                    checked_i16(columns.len(), "too many row description columns"),
                );
                for column in columns {
                    encode_column_info(&mut body, column);
                }
                encode_server_message(b'T', body)
            }
            ServerMessage::DataRow(values) => {
                let mut body = Vec::new();
                put_i16(
                    &mut body,
                    checked_i16(values.len(), "too many data row columns"),
                );
                for value in values {
                    match value {
                        Some(value) => {
                            let bytes = value.as_bytes();
                            put_i32(
                                &mut body,
                                checked_i32(bytes.len(), "data row value too large"),
                            );
                            body.extend_from_slice(bytes);
                        }
                        None => put_i32(&mut body, -1),
                    }
                }
                encode_server_message(b'D', body)
            }
            ServerMessage::CommandComplete(tag) => {
                let mut body = Vec::new();
                put_cstr(&mut body, tag);
                encode_server_message(b'C', body)
            }
            ServerMessage::ErrorResponse {
                severity,
                code,
                message,
            } => {
                let mut body = Vec::new();
                put_error_field(&mut body, b'S', severity);
                put_error_field(&mut body, b'C', code);
                put_error_field(&mut body, b'M', message);
                body.push(0);
                encode_server_message(b'E', body)
            }
        }
    }
}

fn decode_startup_params(bytes: &[u8]) -> Result<(String, Option<String>)> {
    let mut offset = 0;
    let mut user = None;
    let mut database = None;

    loop {
        let key = read_cstr(bytes, &mut offset)?;
        if key.is_empty() {
            break;
        }
        let value = read_cstr(bytes, &mut offset)?;
        match key {
            "user" => user = Some(value.to_string()),
            "database" => database = Some(value.to_string()),
            _ => {}
        }
    }

    let user = user.ok_or_else(|| protocol_error("startup message is missing user"))?;
    Ok((user, database))
}

fn read_cstr<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a str> {
    let start = *offset;
    let relative_nul = bytes
        .get(start..)
        .and_then(|remaining| remaining.iter().position(|byte| *byte == 0))
        .ok_or_else(|| protocol_error("startup parameter is not nul terminated"))?;
    let end = start + relative_nul;
    *offset = end + 1;
    std::str::from_utf8(&bytes[start..end])
        .map_err(|_| protocol_error("startup parameter is not valid UTF-8"))
}

fn decode_nul_terminated_text<'a>(bytes: &'a [u8], error: &'static str) -> Result<&'a str> {
    match bytes.iter().position(|byte| *byte == 0) {
        Some(nul) if nul + 1 == bytes.len() => std::str::from_utf8(&bytes[..nul])
            .map_err(|_| protocol_error("message text is not valid UTF-8")),
        Some(_) => Err(protocol_error("message has bytes after nul terminator")),
        None => Err(protocol_error(error)),
    }
}

fn encode_server_message(tag: u8, body: Vec<u8>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(body.len() + 5);
    bytes.push(tag);
    put_i32(
        &mut bytes,
        checked_i32(body.len() + 4, "server message too large"),
    );
    bytes.extend_from_slice(&body);
    bytes
}

fn encode_column_info(body: &mut Vec<u8>, column: &ColumnInfo) {
    let (type_oid, type_size) = postgres_type(&column.data_type);
    put_cstr(body, &column.name);
    put_i32(body, 0);
    put_i16(body, 0);
    put_i32(body, type_oid);
    put_i16(body, type_size);
    put_i32(body, -1);
    put_i16(body, 0);
}

fn postgres_type(data_type: &DataType) -> (i32, i16) {
    match data_type {
        DataType::Integer => (20, 8),
        DataType::Text => (25, -1),
        DataType::Boolean => (16, 1),
    }
}

fn put_error_field(body: &mut Vec<u8>, field: u8, value: &str) {
    body.push(field);
    put_cstr(body, value);
}

fn put_cstr(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(0);
}

fn put_i16(bytes: &mut Vec<u8>, value: i16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_i32(bytes: &mut Vec<u8>, value: i32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn read_i32(bytes: &[u8]) -> Result<i32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| protocol_error("integer field is incomplete"))?;
    Ok(i32::from_be_bytes(bytes))
}

fn checked_i16(value: usize, message: &'static str) -> i16 {
    i16::try_from(value).expect(message)
}

fn checked_i32(value: usize, message: &'static str) -> i32 {
    i32::try_from(value).expect(message)
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::protocol(SqlState::SyntaxError, message)
}
