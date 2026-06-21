mod codec;
mod messages;
mod state;

pub use codec::{PostgresCodec, ProtocolCodec};
pub use messages::{ClientMessage, ServerMessage};
pub use state::{ConnectionState, PostgresConnectionState};

#[cfg(test)]
mod tests {
    use common::{ColumnInfo, DataType};

    use super::{
        ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
        ServerMessage,
    };

    fn ssl_request_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&80877103i32.to_be_bytes());
        bytes
    }

    fn gssenc_request_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&80877104i32.to_be_bytes());
        bytes
    }

    fn startup_bytes(user: &str, database: Option<&str>) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        if let Some(database) = database {
            body.extend_from_slice(b"database\0");
            body.extend_from_slice(database.as_bytes());
            body.push(0);
        }
        body.extend_from_slice(b"application_name\0psql\0");
        body.push(0);

        let mut packet = Vec::new();
        let length = i32::try_from(body.len() + 4).unwrap();
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(&body);
        packet
    }

    fn query_bytes(sql: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(b'Q');
        let length = i32::try_from(sql.len() + 5).unwrap();
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(sql.as_bytes());
        packet.push(0);
        packet
    }

    fn terminate_bytes() -> Vec<u8> {
        vec![b'X', 0, 0, 0, 4]
    }

    fn column(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            table_id: None,
            column_id: None,
        }
    }

    fn read_i16(bytes: &[u8], offset: &mut usize) -> i16 {
        let value = i16::from_be_bytes([bytes[*offset], bytes[*offset + 1]]);
        *offset += 2;
        value
    }

    fn read_i32(bytes: &[u8], offset: &mut usize) -> i32 {
        let value = i32::from_be_bytes([
            bytes[*offset],
            bytes[*offset + 1],
            bytes[*offset + 2],
            bytes[*offset + 3],
        ]);
        *offset += 4;
        value
    }

    fn read_cstr<'a>(bytes: &'a [u8], offset: &mut usize) -> &'a str {
        let start = *offset;
        let nul = bytes[start..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|relative| start + relative)
            .unwrap();
        *offset = nul + 1;
        std::str::from_utf8(&bytes[start..nul]).unwrap()
    }

    #[test]
    fn decodes_ssl_request_and_encodes_rejection() {
        let mut codec = PostgresCodec::new();
        let messages = codec.decode(&ssl_request_bytes()).unwrap();

        assert_eq!(messages, vec![ClientMessage::SslRequest]);
        assert_eq!(codec.encode(&ServerMessage::SslRejected), b"N".to_vec());
    }

    #[test]
    fn encodes_ssl_accepted_as_single_s_byte() {
        let codec = PostgresCodec::new();
        assert_eq!(codec.encode(&ServerMessage::SslAccepted), b"S".to_vec());
    }

    #[test]
    fn decodes_gssenc_request() {
        let mut codec = PostgresCodec::new();
        assert_eq!(
            codec.decode(&gssenc_request_bytes()).unwrap(),
            vec![ClientMessage::GssEncRequest]
        );
    }

    #[test]
    fn gssenc_request_is_declined_with_rejection_byte() {
        let mut state = PostgresConnectionState::new();
        assert_eq!(
            state.handle_message(ClientMessage::GssEncRequest).unwrap(),
            vec![ServerMessage::SslRejected]
        );
    }

    #[test]
    fn startup_state_emits_authentication_parameters_and_ready() {
        let mut state = PostgresConnectionState::new();
        let messages = state
            .handle_message(ClientMessage::Startup {
                user: "dave".to_string(),
                database: Some("saguaro".to_string()),
                application_name: Some("psql".to_string()),
            })
            .unwrap();

        assert!(matches!(messages[0], ServerMessage::AuthenticationOk));
        assert!(matches!(
            messages.last(),
            Some(ServerMessage::ReadyForQuery)
        ));
        for (key, value) in [
            ("server_version", "16.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("TimeZone", "UTC"),
            ("application_name", "psql"),
        ] {
            assert!(messages.contains(&ServerMessage::ParameterStatus {
                key: key.to_string(),
                value: value.to_string(),
            }));
        }
    }

    #[test]
    fn startup_without_application_name_reports_it_empty() {
        let mut state = PostgresConnectionState::new();
        let messages = state
            .handle_message(ClientMessage::Startup {
                user: "dave".to_string(),
                database: None,
                application_name: None,
            })
            .unwrap();

        assert!(messages.contains(&ServerMessage::ParameterStatus {
            key: "application_name".to_string(),
            value: String::new(),
        }));
    }

    #[test]
    fn data_row_encodes_null_field_as_negative_length() {
        let codec = PostgresCodec::new();
        let bytes = codec.encode(&ServerMessage::DataRow(vec![Some("7".to_string()), None]));

        assert!(
            bytes
                .windows(4)
                .any(|window| window == (-1i32).to_be_bytes())
        );
    }

    #[test]
    fn ready_for_query_encodes_idle_status() {
        let codec = PostgresCodec::new();
        let bytes = codec.encode(&ServerMessage::ReadyForQuery);

        assert_eq!(bytes, vec![b'Z', 0, 0, 0, 5, b'I']);
    }

    #[test]
    fn decodes_startup_message_reads_user_database_application_name_and_buffers_incomplete_input() {
        let mut codec = PostgresCodec::new();
        let packet = startup_bytes("dave", Some("saguaro"));
        let split = packet.len() - 2;

        assert_eq!(codec.decode(&packet[..split]).unwrap(), Vec::new());
        assert_eq!(
            codec.decode(&packet[split..]).unwrap(),
            vec![ClientMessage::Startup {
                user: "dave".to_string(),
                database: Some("saguaro".to_string()),
                application_name: Some("psql".to_string()),
            }]
        );
    }

    #[test]
    fn decodes_simple_query_message() {
        let mut codec = PostgresCodec::new();

        assert_eq!(
            codec.decode(&query_bytes("select 1")).unwrap(),
            vec![ClientMessage::Query("select 1".to_string())]
        );
    }

    #[test]
    fn decode_can_return_many_messages() {
        let mut codec = PostgresCodec::new();
        let mut bytes = query_bytes("select 1");
        bytes.extend_from_slice(&terminate_bytes());

        assert_eq!(
            codec.decode(&bytes).unwrap(),
            vec![
                ClientMessage::Query("select 1".to_string()),
                ClientMessage::Terminate,
            ]
        );
    }

    #[test]
    fn unsupported_tagged_message_returns_protocol_error() {
        let mut codec = PostgresCodec::new();
        let err = codec.decode(&[b'S', 0, 0, 0, 4]).unwrap_err();

        assert_eq!(err.code, common::SqlState::SyntaxError);
    }

    #[test]
    fn oversized_startup_length_returns_protocol_error_without_buffering_forever() {
        let mut codec = PostgresCodec::new();
        let err = codec.decode(&(1_048_577i32).to_be_bytes()).unwrap_err();

        assert_eq!(err.code, common::SqlState::SyntaxError);
    }

    #[test]
    fn oversized_tagged_length_returns_protocol_error_without_buffering_forever() {
        let mut codec = PostgresCodec::new();
        let mut bytes = vec![b'Q'];
        bytes.extend_from_slice(&(1_048_577i32).to_be_bytes());

        let err = codec.decode(&bytes).unwrap_err();

        assert_eq!(err.code, common::SqlState::SyntaxError);
    }

    #[test]
    fn query_rejects_embedded_nul_trailing_payload() {
        let mut codec = PostgresCodec::new();
        let mut bytes = vec![b'Q'];
        let payload = b"select 1\0junk\0";
        let length = i32::try_from(payload.len() + 4).unwrap();
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(payload);

        let err = codec.decode(&bytes).unwrap_err();

        assert_eq!(err.code, common::SqlState::SyntaxError);
    }

    #[test]
    fn row_description_encodes_v1_type_oids_sizes_and_text_format() {
        let codec = PostgresCodec::new();
        let bytes = codec.encode(&ServerMessage::RowDescription(vec![
            column("id", DataType::Integer),
            column("name", DataType::Text),
            column("active", DataType::Boolean),
        ]));

        assert_eq!(bytes[0], b'T');
        assert_eq!(
            i32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize,
            bytes.len() - 1
        );
        let mut offset = 5;
        assert_eq!(read_i16(&bytes, &mut offset), 3);

        for (name, oid, size) in [("id", 20, 8), ("name", 25, -1), ("active", 16, 1)] {
            assert_eq!(read_cstr(&bytes, &mut offset), name);
            assert_eq!(read_i32(&bytes, &mut offset), 0);
            assert_eq!(read_i16(&bytes, &mut offset), 0);
            assert_eq!(read_i32(&bytes, &mut offset), oid);
            assert_eq!(read_i16(&bytes, &mut offset), size);
            assert_eq!(read_i32(&bytes, &mut offset), -1);
            assert_eq!(read_i16(&bytes, &mut offset), 0);
        }
        assert_eq!(offset, bytes.len());
    }

    #[test]
    fn error_response_encodes_sqlstate_severity_and_message_fields() {
        let codec = PostgresCodec::new();
        let bytes = codec.encode(&ServerMessage::ErrorResponse {
            severity: "ERROR".to_string(),
            code: "42P01".to_string(),
            message: "table not found".to_string(),
        });

        assert_eq!(bytes[0], b'E');
        assert_eq!(
            i32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize,
            bytes.len() - 1
        );
        assert!(bytes.windows(7).any(|window| window == b"SERROR\0"));
        assert!(bytes.windows(7).any(|window| window == b"C42P01\0"));
        assert!(
            bytes
                .windows(17)
                .any(|window| window == b"Mtable not found\0")
        );
        assert_eq!(bytes.last(), Some(&0));
    }

    #[test]
    fn terminate_marks_connection_terminated() {
        let mut state = PostgresConnectionState::new();

        assert_eq!(
            state.handle_message(ClientMessage::Terminate).unwrap(),
            Vec::new()
        );
        assert!(state.is_terminated());
    }

    #[test]
    fn query_state_returns_no_messages_and_keeps_connection_open() {
        let mut state = PostgresConnectionState::new();

        assert_eq!(
            state
                .handle_message(ClientMessage::Query("select 1".to_string()))
                .unwrap(),
            Vec::new()
        );
        assert!(!state.is_terminated());
    }
}
