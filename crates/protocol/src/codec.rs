use bytes::BytesMut;
use common::{ColumnInfo, DataType, DbError, Result, SqlState, Value};

use crate::{ClientMessage, ServerMessage, StatementKind};

const SSL_REQUEST_CODE: i32 = 80_877_103;
const GSSENC_REQUEST_CODE: i32 = 80_877_104;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
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
        if length == 8 && code == GSSENC_REQUEST_CODE {
            messages.push(ClientMessage::GssEncRequest);
            return Ok(true);
        }
        if length == 16 && code == CANCEL_REQUEST_CODE {
            let process_id = read_i32(&packet[8..12])?;
            let secret_key = read_i32(&packet[12..16])?;
            messages.push(ClientMessage::CancelRequest {
                process_id,
                secret_key,
            });
            return Ok(true);
        }
        if code != POSTGRES_PROTOCOL_V3 {
            return Err(protocol_error("unsupported PostgreSQL startup protocol"));
        }

        let (user, database, application_name) = decode_startup_params(&packet[8..])?;
        messages.push(ClientMessage::Startup {
            user,
            database,
            application_name,
        });
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
            b'P' => messages.push(decode_parse(body)?),
            b'B' => messages.push(decode_bind(body)?),
            b'D' => messages.push(decode_describe(body)?),
            b'E' => messages.push(decode_execute(body)?),
            b'C' => messages.push(decode_close(body)?),
            b'S' => {
                if length != 4 {
                    return Err(protocol_error("sync message has invalid length"));
                }
                messages.push(ClientMessage::Sync);
            }
            b'H' => {
                if length != 4 {
                    return Err(protocol_error("flush message has invalid length"));
                }
                messages.push(ClientMessage::Flush);
            }
            b'd' => messages.push(ClientMessage::CopyData(body.to_vec())),
            b'c' => {
                if length != 4 {
                    return Err(protocol_error("CopyDone message has invalid length"));
                }
                messages.push(ClientMessage::CopyDone);
            }
            b'f' => {
                let message =
                    decode_nul_terminated_text(body, "CopyFail message is not nul terminated")?;
                messages.push(ClientMessage::CopyFail(message.to_string()));
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
                b'Q' | b'X' | b'P' | b'B' | b'D' | b'E' | b'C' | b'H' | b'S' | b'd' | b'c'
                | b'f' => self.decode_tagged(&mut messages)?,
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
            ServerMessage::BackendKeyData {
                process_id,
                secret_key,
            } => {
                let mut body = Vec::new();
                put_i32(&mut body, *process_id);
                put_i32(&mut body, *secret_key);
                encode_server_message(b'K', body)
            }
            ServerMessage::ParameterStatus { key, value } => {
                let mut body = Vec::new();
                put_cstr(&mut body, key);
                put_cstr(&mut body, value);
                encode_server_message(b'S', body)
            }
            ServerMessage::ReadyForQuery(status) => encode_server_message(b'Z', vec![*status]),
            ServerMessage::RowDescription { columns, formats } => {
                let mut body = Vec::new();
                put_i16(
                    &mut body,
                    checked_i16(columns.len(), "too many row description columns"),
                );
                for (index, column) in columns.iter().enumerate() {
                    let format = formats.get(index).copied().unwrap_or(0);
                    encode_column_info(&mut body, column, format);
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
                        Some(bytes) => {
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
            ServerMessage::ParseComplete => encode_server_message(b'1', Vec::new()),
            ServerMessage::BindComplete => encode_server_message(b'2', Vec::new()),
            ServerMessage::CloseComplete => encode_server_message(b'3', Vec::new()),
            ServerMessage::ParameterDescription(type_oids) => {
                let mut body = Vec::new();
                put_i16(
                    &mut body,
                    checked_i16(type_oids.len(), "too many parameter descriptions"),
                );
                for oid in type_oids {
                    put_i32(&mut body, *oid);
                }
                encode_server_message(b't', body)
            }
            ServerMessage::NoData => encode_server_message(b'n', Vec::new()),
            ServerMessage::CopyInResponse {
                overall_format,
                column_formats,
            } => encode_server_message(b'G', encode_copy_response(*overall_format, column_formats)),
            ServerMessage::CopyOutResponse {
                overall_format,
                column_formats,
            } => encode_server_message(b'H', encode_copy_response(*overall_format, column_formats)),
            ServerMessage::CopyData(bytes) => encode_server_message(b'd', bytes.clone()),
            ServerMessage::CopyDone => encode_server_message(b'c', Vec::new()),
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

fn decode_startup_params(bytes: &[u8]) -> Result<(String, Option<String>, Option<String>)> {
    let mut offset = 0;
    let mut user = None;
    let mut database = None;
    let mut application_name = None;

    loop {
        let key = read_cstr(bytes, &mut offset)?;
        if key.is_empty() {
            break;
        }
        let value = read_cstr(bytes, &mut offset)?;
        match key {
            "user" => user = Some(value.to_string()),
            "database" => database = Some(value.to_string()),
            "application_name" => application_name = Some(value.to_string()),
            _ => {}
        }
    }

    let user = user.ok_or_else(|| protocol_error("startup message is missing user"))?;
    Ok((user, database, application_name))
}

fn read_cstr<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a str> {
    let start = *offset;
    let relative_nul = bytes
        .get(start..)
        .and_then(|remaining| remaining.iter().position(|byte| *byte == 0))
        .ok_or_else(|| protocol_error("string field is not nul terminated"))?;
    let end = start + relative_nul;
    *offset = end + 1;
    std::str::from_utf8(&bytes[start..end])
        .map_err(|_| protocol_error("string field is not valid UTF-8"))
}

fn decode_parse(body: &[u8]) -> Result<ClientMessage> {
    let mut offset = 0;
    let name = read_cstr(body, &mut offset)?.to_string();
    let query = read_cstr(body, &mut offset)?.to_string();
    let count = read_count(body, &mut offset, "parse parameter type")?;
    let mut param_types = Vec::with_capacity(count);
    for _ in 0..count {
        param_types.push(read_i32_at(body, &mut offset)?);
    }
    require_consumed(body, offset)?;
    Ok(ClientMessage::Parse {
        name,
        query,
        param_types,
    })
}

fn decode_bind(body: &[u8]) -> Result<ClientMessage> {
    let mut offset = 0;
    let portal = read_cstr(body, &mut offset)?.to_string();
    let statement = read_cstr(body, &mut offset)?.to_string();
    let param_formats = read_i16_array(body, &mut offset, "bind parameter format")?;
    let param_count = read_count(body, &mut offset, "bind parameter")?;
    let mut params = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        params.push(read_param_value(body, &mut offset)?);
    }
    let result_formats = read_i16_array(body, &mut offset, "bind result format")?;
    require_consumed(body, offset)?;
    Ok(ClientMessage::Bind {
        portal,
        statement,
        param_formats,
        params,
        result_formats,
    })
}

fn decode_describe(body: &[u8]) -> Result<ClientMessage> {
    let mut offset = 0;
    let kind = read_kind(body, &mut offset)?;
    let name = read_cstr(body, &mut offset)?.to_string();
    require_consumed(body, offset)?;
    Ok(ClientMessage::Describe { kind, name })
}

fn decode_close(body: &[u8]) -> Result<ClientMessage> {
    let mut offset = 0;
    let kind = read_kind(body, &mut offset)?;
    let name = read_cstr(body, &mut offset)?.to_string();
    require_consumed(body, offset)?;
    Ok(ClientMessage::Close { kind, name })
}

fn decode_execute(body: &[u8]) -> Result<ClientMessage> {
    let mut offset = 0;
    let portal = read_cstr(body, &mut offset)?.to_string();
    let max_rows = read_i32_at(body, &mut offset)?;
    require_consumed(body, offset)?;
    Ok(ClientMessage::Execute { portal, max_rows })
}

fn read_kind(bytes: &[u8], offset: &mut usize) -> Result<StatementKind> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| protocol_error("describe/close message is truncated"))?;
    *offset += 1;
    match tag {
        b'S' => Ok(StatementKind::Statement),
        b'P' => Ok(StatementKind::Portal),
        _ => Err(protocol_error("describe/close target must be 'S' or 'P'")),
    }
}

fn read_i16_array(bytes: &[u8], offset: &mut usize, what: &str) -> Result<Vec<i16>> {
    let count = read_count(bytes, offset, what)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_i16_at(bytes, offset)?);
    }
    Ok(values)
}

fn read_param_value(bytes: &[u8], offset: &mut usize) -> Result<Option<Vec<u8>>> {
    let length = read_i32_at(bytes, offset)?;
    if length == -1 {
        return Ok(None);
    }
    let length =
        usize::try_from(length).map_err(|_| protocol_error("bind parameter has invalid length"))?;
    let start = *offset;
    let end = start
        .checked_add(length)
        .ok_or_else(|| protocol_error("bind parameter length overflows"))?;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| protocol_error("bind parameter is truncated"))?;
    *offset = end;
    Ok(Some(slice.to_vec()))
}

/// Read a non-negative `int16` count field.
fn read_count(bytes: &[u8], offset: &mut usize, what: &str) -> Result<usize> {
    let count = read_i16_at(bytes, offset)?;
    usize::try_from(count).map_err(|_| protocol_error(format!("{what} count is negative")))
}

fn read_i16_at(bytes: &[u8], offset: &mut usize) -> Result<i16> {
    let start = *offset;
    let end = start + 2;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| protocol_error("message is truncated reading int16"))?;
    *offset = end;
    Ok(i16::from_be_bytes([slice[0], slice[1]]))
}

fn read_i32_at(bytes: &[u8], offset: &mut usize) -> Result<i32> {
    let start = *offset;
    let end = start + 4;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| protocol_error("message is truncated reading int32"))?;
    *offset = end;
    Ok(i32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn require_consumed(body: &[u8], offset: usize) -> Result<()> {
    if offset != body.len() {
        return Err(protocol_error("message has unexpected trailing bytes"));
    }
    Ok(())
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

/// Body shared by `CopyInResponse`/`CopyOutResponse`: `int8 overall_format`,
/// `int16 column_count`, then one `int16 format_code` per column.
fn encode_copy_response(overall_format: i8, column_formats: &[i16]) -> Vec<u8> {
    let mut body = Vec::with_capacity(3 + column_formats.len() * 2);
    body.push(overall_format as u8);
    put_i16(
        &mut body,
        checked_i16(column_formats.len(), "too many copy columns"),
    );
    for format in column_formats {
        put_i16(&mut body, *format);
    }
    body
}

fn encode_column_info(body: &mut Vec<u8>, column: &ColumnInfo, format: i16) {
    let (type_oid, type_size) = postgres_type(&column.data_type);
    put_cstr(body, &column.name);
    put_i32(body, 0);
    put_i16(body, 0);
    put_i32(body, type_oid);
    put_i16(body, type_size);
    put_i32(body, -1);
    put_i16(body, format);
}

/// PostgreSQL wire format code for a single value: text (`0`) or binary (`1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueFormat {
    Text,
    Binary,
}

impl ValueFormat {
    fn from_code(code: i16) -> Result<Self> {
        match code {
            0 => Ok(ValueFormat::Text),
            1 => Ok(ValueFormat::Binary),
            _ => Err(protocol_error("unsupported value format code")),
        }
    }
}

/// Encode a value to its PostgreSQL wire bytes in the given format code (`0` =
/// text, `1` = binary), or `None` for SQL NULL. For `TEXT` the text and binary
/// encodings are identical (raw UTF-8 bytes).
pub fn encode_value(value: &Value, format: i16) -> Result<Option<Vec<u8>>> {
    let format = ValueFormat::from_code(format)?;
    let bytes = match value {
        Value::Null => return Ok(None),
        Value::Boolean(flag) => match format {
            ValueFormat::Text => {
                if *flag {
                    b"t".to_vec()
                } else {
                    b"f".to_vec()
                }
            }
            ValueFormat::Binary => vec![u8::from(*flag)],
        },
        Value::Integer(int) => match format {
            ValueFormat::Text => int.to_string().into_bytes(),
            ValueFormat::Binary => int.to_be_bytes().to_vec(),
        },
        Value::Text(text) => text.clone().into_bytes(),
        Value::Date(days) => match format {
            ValueFormat::Text => common::datetime::format_date(*days).into_bytes(),
            // PostgreSQL binary `date` is an i32 day count from 2000-01-01.
            ValueFormat::Binary => date_to_pg_binary(*days)?.to_be_bytes().to_vec(),
        },
        Value::Timestamp(micros) => match format {
            ValueFormat::Text => common::datetime::format_timestamp(*micros).into_bytes(),
            // PostgreSQL binary `timestamp` is an i64 microsecond count from 2000-01-01.
            ValueFormat::Binary => (micros - PG_TIMESTAMP_EPOCH_OFFSET_MICROS)
                .to_be_bytes()
                .to_vec(),
        },
        Value::Bytes(raw) => match format {
            // PostgreSQL text `bytea` is the hex format `\x...`; binary is the raw bytes.
            ValueFormat::Text => common::bytea::format_hex(raw).into_bytes(),
            ValueFormat::Binary => raw.clone(),
        },
        Value::Uuid(raw) => match format {
            ValueFormat::Text => common::uuid::format_uuid(raw).into_bytes(),
            ValueFormat::Binary => raw.to_vec(),
        },
    };
    Ok(Some(bytes))
}

/// Days from the Unix epoch (1970-01-01) to the PostgreSQL date epoch
/// (2000-01-01), used to convert our internal day count to/from the wire's
/// binary `date` representation.
const PG_DATE_EPOCH_OFFSET_DAYS: i64 = 10957;

/// Microseconds from the Unix epoch (1970-01-01) to the PostgreSQL timestamp
/// epoch (2000-01-01), used for the binary `timestamp` wire representation.
const PG_TIMESTAMP_EPOCH_OFFSET_MICROS: i64 = 10957 * 86_400 * 1_000_000;

fn date_to_pg_binary(days: i64) -> Result<i32> {
    i32::try_from(days - PG_DATE_EPOCH_OFFSET_DAYS)
        .map_err(|_| protocol_error("date is out of range for the binary wire format"))
}

/// Decode a non-NULL parameter's wire bytes into a `Value` of the target type,
/// using the given format code. (SQL NULL is represented by the absence of a
/// value at the `Bind` layer, so it never reaches here.)
pub fn decode_value(bytes: &[u8], data_type: DataType, format: i16) -> Result<Value> {
    let format = ValueFormat::from_code(format)?;
    match (data_type, format) {
        (DataType::Integer, ValueFormat::Text) => {
            let text = decode_utf8(bytes, "integer parameter")?;
            let int = text
                .trim()
                .parse::<i64>()
                .map_err(|_| protocol_error("invalid integer parameter"))?;
            Ok(Value::Integer(int))
        }
        (DataType::Integer, ValueFormat::Binary) => {
            let array: [u8; 8] = bytes
                .try_into()
                .map_err(|_| protocol_error("binary integer parameter must be 8 bytes"))?;
            Ok(Value::Integer(i64::from_be_bytes(array)))
        }
        (DataType::Boolean, ValueFormat::Text) => {
            let text = decode_utf8(bytes, "boolean parameter")?;
            parse_bool_text(text)
        }
        (DataType::Boolean, ValueFormat::Binary) => match bytes {
            [0] => Ok(Value::Boolean(false)),
            [1] => Ok(Value::Boolean(true)),
            _ => Err(protocol_error(
                "binary boolean parameter must be a single 0 or 1 byte",
            )),
        },
        (DataType::Text, _) => Ok(Value::Text(
            decode_utf8(bytes, "text parameter")?.to_string(),
        )),
        (DataType::Date, ValueFormat::Text) => {
            let text = decode_utf8(bytes, "date parameter")?;
            common::datetime::parse_date(text)
                .map(Value::Date)
                .ok_or_else(|| protocol_error("invalid date parameter"))
        }
        (DataType::Date, ValueFormat::Binary) => {
            let array: [u8; 4] = bytes
                .try_into()
                .map_err(|_| protocol_error("binary date parameter must be 4 bytes"))?;
            Ok(Value::Date(
                i32::from_be_bytes(array) as i64 + PG_DATE_EPOCH_OFFSET_DAYS,
            ))
        }
        (DataType::Timestamp, ValueFormat::Text) => {
            let text = decode_utf8(bytes, "timestamp parameter")?;
            common::datetime::parse_timestamp(text)
                .map(Value::Timestamp)
                .ok_or_else(|| protocol_error("invalid timestamp parameter"))
        }
        (DataType::Timestamp, ValueFormat::Binary) => {
            let array: [u8; 8] = bytes
                .try_into()
                .map_err(|_| protocol_error("binary timestamp parameter must be 8 bytes"))?;
            Ok(Value::Timestamp(
                i64::from_be_bytes(array) + PG_TIMESTAMP_EPOCH_OFFSET_MICROS,
            ))
        }
        (DataType::Bytea, ValueFormat::Text) => {
            let text = decode_utf8(bytes, "bytea parameter")?;
            common::bytea::parse_hex(text)
                .map(Value::Bytes)
                .ok_or_else(|| protocol_error("invalid bytea parameter"))
        }
        (DataType::Bytea, ValueFormat::Binary) => Ok(Value::Bytes(bytes.to_vec())),
        (DataType::Uuid, ValueFormat::Text) => {
            let text = decode_utf8(bytes, "uuid parameter")?;
            common::uuid::parse_uuid(text)
                .map(Value::Uuid)
                .ok_or_else(|| protocol_error("invalid uuid parameter"))
        }
        (DataType::Uuid, ValueFormat::Binary) => {
            let array: [u8; 16] = bytes
                .try_into()
                .map_err(|_| protocol_error("binary uuid parameter must be 16 bytes"))?;
            Ok(Value::Uuid(array))
        }
    }
}

fn decode_utf8<'a>(bytes: &'a [u8], what: &str) -> Result<&'a str> {
    std::str::from_utf8(bytes).map_err(|_| protocol_error(format!("{what} is not valid UTF-8")))
}

fn parse_bool_text(text: &str) -> Result<Value> {
    common::parse_bool_text(text)
        .map(Value::Boolean)
        .ok_or_else(|| protocol_error("invalid boolean parameter"))
}

fn postgres_type(data_type: &DataType) -> (i32, i16) {
    match data_type {
        DataType::Integer => (20, 8),
        DataType::Text => (25, -1),
        DataType::Boolean => (16, 1),
        DataType::Date => (1082, 4),
        DataType::Timestamp => (1114, 8),
        DataType::Bytea => (17, -1),
        DataType::Uuid => (2950, 16),
    }
}

/// PostgreSQL type OID for a v1 data type (e.g. for `ParameterDescription`).
pub fn type_oid(data_type: &DataType) -> i32 {
    postgres_type(data_type).0
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

#[cfg(test)]
mod date_value_tests {
    use super::{decode_value, encode_value};
    use common::{DataType, Value};

    #[test]
    fn date_text_round_trips_and_formats() {
        // 2000-01-01 is exactly the PG date epoch (10957 days from 1970-01-01).
        let value = Value::Date(10957);
        let text = encode_value(&value, 0).unwrap().unwrap();
        assert_eq!(text, b"2000-01-01");
        assert_eq!(decode_value(&text, DataType::Date, 0).unwrap(), value);
    }

    #[test]
    fn date_binary_uses_pg_2000_epoch_and_round_trips() {
        // Binary date is i32 BE days-from-2000, so 2000-01-01 encodes to 0.
        let value = Value::Date(10957);
        let binary = encode_value(&value, 1).unwrap().unwrap();
        assert_eq!(binary, 0_i32.to_be_bytes());
        assert_eq!(decode_value(&binary, DataType::Date, 1).unwrap(), value);

        // A day before the epoch is -1.
        let value = Value::Date(10956);
        let binary = encode_value(&value, 1).unwrap().unwrap();
        assert_eq!(binary, (-1_i32).to_be_bytes());
        assert_eq!(decode_value(&binary, DataType::Date, 1).unwrap(), value);
    }

    #[test]
    fn invalid_date_text_is_rejected() {
        assert!(decode_value(b"2023-02-29", DataType::Date, 0).is_err());
    }
}

#[cfg(test)]
mod timestamp_value_tests {
    use super::{decode_value, encode_value};
    use common::{DataType, Value};

    // 2000-01-01 00:00:00 is the PG timestamp epoch: 10957 days of micros from 1970.
    const PG_EPOCH_MICROS: i64 = 10957 * 86_400 * 1_000_000;

    #[test]
    fn timestamp_text_round_trips_and_formats() {
        let value = Value::Timestamp(PG_EPOCH_MICROS + 45 * 1_000_000);
        let text = encode_value(&value, 0).unwrap().unwrap();
        assert_eq!(text, b"2000-01-01 00:00:45");
        assert_eq!(decode_value(&text, DataType::Timestamp, 0).unwrap(), value);
    }

    #[test]
    fn timestamp_binary_uses_pg_2000_epoch_and_round_trips() {
        let value = Value::Timestamp(PG_EPOCH_MICROS);
        let binary = encode_value(&value, 1).unwrap().unwrap();
        assert_eq!(binary, 0_i64.to_be_bytes());
        assert_eq!(
            decode_value(&binary, DataType::Timestamp, 1).unwrap(),
            value
        );

        let value = Value::Timestamp(PG_EPOCH_MICROS - 1_000_000); // one second before
        let binary = encode_value(&value, 1).unwrap().unwrap();
        assert_eq!(binary, (-1_000_000_i64).to_be_bytes());
        assert_eq!(
            decode_value(&binary, DataType::Timestamp, 1).unwrap(),
            value
        );
    }

    #[test]
    fn invalid_timestamp_text_is_rejected() {
        assert!(decode_value(b"2024-01-15 25:00:00", DataType::Timestamp, 0).is_err());
    }
}

#[cfg(test)]
mod bytea_value_tests {
    use super::{decode_value, encode_value};
    use common::{DataType, Value};

    #[test]
    fn bytea_text_is_hex_and_round_trips() {
        let value = Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]);
        let text = encode_value(&value, 0).unwrap().unwrap();
        assert_eq!(text, b"\\xdeadbeef");
        assert_eq!(decode_value(&text, DataType::Bytea, 0).unwrap(), value);
        // Empty bytea is `\x`.
        assert_eq!(
            encode_value(&Value::Bytes(vec![]), 0).unwrap().unwrap(),
            b"\\x"
        );
    }

    #[test]
    fn bytea_binary_is_raw_bytes_and_round_trips() {
        let value = Value::Bytes(vec![0x00, 0xff, 0x10]);
        let binary = encode_value(&value, 1).unwrap().unwrap();
        assert_eq!(binary, vec![0x00, 0xff, 0x10]);
        assert_eq!(decode_value(&binary, DataType::Bytea, 1).unwrap(), value);
    }

    #[test]
    fn invalid_bytea_text_is_rejected() {
        assert!(decode_value(b"\\xabc", DataType::Bytea, 0).is_err()); // odd length
        assert!(decode_value(b"nothex", DataType::Bytea, 0).is_err());
    }
}

#[cfg(test)]
mod uuid_value_tests {
    use super::{decode_value, encode_value};
    use common::{DataType, Value};

    const SAMPLE: [u8; 16] = [
        0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x19,
    ];

    #[test]
    fn uuid_text_is_canonical_and_round_trips() {
        let value = Value::Uuid(SAMPLE);
        let text = encode_value(&value, 0).unwrap().unwrap();
        assert_eq!(text, b"0a0b0c0d-0e0f-1011-1213-141516171819");
        assert_eq!(decode_value(&text, DataType::Uuid, 0).unwrap(), value);
    }

    #[test]
    fn uuid_binary_is_16_raw_bytes_and_round_trips() {
        let value = Value::Uuid(SAMPLE);
        let binary = encode_value(&value, 1).unwrap().unwrap();
        assert_eq!(binary, SAMPLE.to_vec());
        assert_eq!(decode_value(&binary, DataType::Uuid, 1).unwrap(), value);
    }

    #[test]
    fn invalid_uuid_is_rejected() {
        assert!(decode_value(b"not-a-uuid", DataType::Uuid, 0).is_err());
        assert!(decode_value(&[0u8; 15], DataType::Uuid, 1).is_err()); // wrong length
    }
}
