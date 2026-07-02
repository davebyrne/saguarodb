mod codec;
mod messages;
mod state;

pub use codec::{PostgresCodec, ProtocolCodec, decode_value, encode_value, encode_value_with_type};
pub use messages::{ClientMessage, ServerMessage, StatementKind};
pub use state::{ConnectionState, PostgresConnectionState};

#[cfg(test)]
mod tests {
    use common::{ColumnInfo, DataType, PgType, Value};

    use super::{
        ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
        ServerMessage, StatementKind, decode_value, encode_value, encode_value_with_type,
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

    fn cancel_request_bytes(process_id: i32, secret_key: i32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&16i32.to_be_bytes());
        bytes.extend_from_slice(&80877102i32.to_be_bytes());
        bytes.extend_from_slice(&process_id.to_be_bytes());
        bytes.extend_from_slice(&secret_key.to_be_bytes());
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
            pg_type: None,
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
    fn decodes_cancel_request() {
        let mut codec = PostgresCodec::new();
        assert_eq!(
            codec.decode(&cancel_request_bytes(42, 1_234_567)).unwrap(),
            vec![ClientMessage::CancelRequest {
                process_id: 42,
                secret_key: 1_234_567,
            }]
        );
    }

    #[test]
    fn encodes_backend_key_data() {
        let codec = PostgresCodec::new();
        let bytes = codec.encode(&ServerMessage::BackendKeyData {
            process_id: 7,
            secret_key: 99,
        });
        assert_eq!(bytes[0], b'K');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 12);
        let mut offset = 5;
        assert_eq!(read_i32(&bytes, &mut offset), 7);
        assert_eq!(read_i32(&bytes, &mut offset), 99);
    }

    fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut packet = vec![tag];
        let length = i32::try_from(body.len() + 4).unwrap();
        packet.extend_from_slice(&length.to_be_bytes());
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

    fn describe_bytes(kind: u8, name: &str) -> Vec<u8> {
        let mut body = vec![kind];
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        tagged(b'D', &body)
    }

    fn close_bytes(kind: u8, name: &str) -> Vec<u8> {
        let mut body = vec![kind];
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        tagged(b'C', &body)
    }

    fn execute_bytes(portal: &str, max_rows: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(&max_rows.to_be_bytes());
        tagged(b'E', &body)
    }

    #[test]
    fn decodes_parse_message_with_param_type() {
        let mut codec = PostgresCodec::new();
        let messages = codec
            .decode(&parse_bytes(
                "stmt1",
                "select id from t where id = $1",
                &[20],
            ))
            .unwrap();
        assert_eq!(
            messages,
            vec![ClientMessage::Parse {
                name: "stmt1".to_string(),
                query: "select id from t where id = $1".to_string(),
                param_types: vec![20],
            }]
        );
    }

    #[test]
    fn decodes_bind_message_with_text_and_null_params() {
        let mut codec = PostgresCodec::new();
        let bytes = bind_bytes("", "stmt1", &[0], &[Some(b"42"), None], &[0, 1]);
        let messages = codec.decode(&bytes).unwrap();
        assert_eq!(
            messages,
            vec![ClientMessage::Bind {
                portal: String::new(),
                statement: "stmt1".to_string(),
                param_formats: vec![0],
                params: vec![Some(b"42".to_vec()), None],
                result_formats: vec![0, 1],
            }]
        );
    }

    #[test]
    fn decodes_describe_execute_and_close() {
        let mut codec = PostgresCodec::new();
        assert_eq!(
            codec.decode(&describe_bytes(b'S', "stmt1")).unwrap(),
            vec![ClientMessage::Describe {
                kind: StatementKind::Statement,
                name: "stmt1".to_string(),
            }]
        );
        assert_eq!(
            codec.decode(&execute_bytes("", 0)).unwrap(),
            vec![ClientMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            }]
        );
        assert_eq!(
            codec.decode(&close_bytes(b'P', "p1")).unwrap(),
            vec![ClientMessage::Close {
                kind: StatementKind::Portal,
                name: "p1".to_string(),
            }]
        );
    }

    #[test]
    fn decodes_sync_and_flush() {
        let mut codec = PostgresCodec::new();
        assert_eq!(
            codec.decode(&tagged(b'S', &[])).unwrap(),
            vec![ClientMessage::Sync]
        );
        assert_eq!(
            codec.decode(&tagged(b'H', &[])).unwrap(),
            vec![ClientMessage::Flush]
        );
    }

    #[test]
    fn describe_with_invalid_kind_is_protocol_error() {
        let mut codec = PostgresCodec::new();
        let err = codec.decode(&describe_bytes(b'Z', "x")).unwrap_err();
        assert_eq!(err.code, common::SqlState::SyntaxError);
    }

    #[test]
    fn decodes_full_extended_sequence_in_one_buffer() {
        let mut codec = PostgresCodec::new();
        let mut bytes = parse_bytes("s", "select 1", &[]);
        bytes.extend_from_slice(&bind_bytes("", "s", &[], &[], &[]));
        bytes.extend_from_slice(&describe_bytes(b'P', ""));
        bytes.extend_from_slice(&execute_bytes("", 0));
        bytes.extend_from_slice(&tagged(b'S', &[]));

        let messages = codec.decode(&bytes).unwrap();
        assert_eq!(messages.len(), 5);
        assert!(matches!(messages[0], ClientMessage::Parse { .. }));
        assert!(matches!(messages[1], ClientMessage::Bind { .. }));
        assert!(matches!(messages[4], ClientMessage::Sync));
    }

    #[test]
    fn encodes_extended_completion_messages_as_empty_tagged_frames() {
        let codec = PostgresCodec::new();
        assert_eq!(
            codec.encode(&ServerMessage::ParseComplete),
            vec![b'1', 0, 0, 0, 4]
        );
        assert_eq!(
            codec.encode(&ServerMessage::BindComplete),
            vec![b'2', 0, 0, 0, 4]
        );
        assert_eq!(
            codec.encode(&ServerMessage::CloseComplete),
            vec![b'3', 0, 0, 0, 4]
        );
        assert_eq!(codec.encode(&ServerMessage::NoData), vec![b'n', 0, 0, 0, 4]);
    }

    #[test]
    fn encodes_parameter_description_with_type_oids() {
        let codec = PostgresCodec::new();
        let bytes = codec.encode(&ServerMessage::ParameterDescription(vec![20, 25]));

        assert_eq!(bytes[0], b't');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 14);
        let mut offset = 5;
        assert_eq!(read_i16(&bytes, &mut offset), 2);
        assert_eq!(read_i32(&bytes, &mut offset), 20);
        assert_eq!(read_i32(&bytes, &mut offset), 25);
    }

    fn value_cases() -> Vec<(Value, DataType)> {
        vec![
            (Value::Integer(-42), DataType::Integer),
            (Value::Text("hello".to_string()), DataType::Text),
            (Value::Boolean(true), DataType::Boolean),
            (Value::Boolean(false), DataType::Boolean),
        ]
    }

    #[test]
    fn value_codec_round_trips_text_and_binary() {
        for format in [0i16, 1] {
            for (value, data_type) in value_cases() {
                let bytes = encode_value(&value, format).unwrap().unwrap();
                let decoded = decode_value(&bytes, data_type, format).unwrap();
                assert_eq!(decoded, value, "format {format}");
            }
        }
    }

    #[test]
    fn encode_value_null_is_none_in_both_formats() {
        assert_eq!(encode_value(&Value::Null, 0).unwrap(), None);
        assert_eq!(encode_value(&Value::Null, 1).unwrap(), None);
    }

    #[test]
    fn binary_integer_encodes_as_eight_byte_big_endian() {
        let bytes = encode_value(&Value::Integer(1), 1).unwrap().unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn text_value_encoding_is_identical_in_both_formats() {
        let text = Value::Text("abc".to_string());
        assert_eq!(
            encode_value(&text, 0).unwrap(),
            encode_value(&text, 1).unwrap()
        );
    }

    #[test]
    fn decode_value_rejects_malformed_input() {
        // Binary integer must be 2, 4, or 8 bytes (3 is malformed).
        assert!(decode_value(&[0, 0, 0], DataType::Integer, 1).is_err());
        // Binary bool must be a single 0/1 byte.
        assert!(decode_value(&[2], DataType::Boolean, 1).is_err());
        // Unparseable text integer.
        assert!(decode_value(b"x", DataType::Integer, 0).is_err());
        // Unsupported format code.
        assert!(decode_value(b"x", DataType::Text, 2).is_err());
        assert!(encode_value(&Value::Integer(1), 2).is_err());
    }

    #[test]
    fn decode_value_accepts_common_text_boolean_forms() {
        assert_eq!(
            decode_value(b"TRUE", DataType::Boolean, 0).unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            decode_value(b"off", DataType::Boolean, 0).unwrap(),
            Value::Boolean(false)
        );
        assert!(decode_value(b"maybe", DataType::Boolean, 0).is_err());
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
            Some(ServerMessage::ReadyForQuery(b'I'))
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
        let bytes = codec.encode(&ServerMessage::DataRow(vec![Some(b"7".to_vec()), None]));

        assert!(
            bytes
                .windows(4)
                .any(|window| window == (-1i32).to_be_bytes())
        );
    }

    #[test]
    fn ready_for_query_encodes_supplied_status_byte() {
        let codec = PostgresCodec::new();
        for status in [b'I', b'T', b'E'] {
            let bytes = codec.encode(&ServerMessage::ReadyForQuery(status));
            assert_eq!(bytes, vec![b'Z', 0, 0, 0, 5, status]);
        }
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
        // 'p' (password) is a frontend tag the v1 server does not support.
        let err = codec.decode(&[b'p', 0, 0, 0, 4]).unwrap_err();

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
        let bytes = codec.encode(&ServerMessage::RowDescription {
            columns: vec![
                column("id", DataType::Integer),
                column("name", DataType::Text),
                column("active", DataType::Boolean),
            ],
            formats: Vec::new(),
        });

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
    fn row_description_reports_declared_oid_typlen_and_typmod() {
        let codec = PostgresCodec::new();
        let labeled = |name: &str, data_type, pg_type| ColumnInfo {
            name: name.to_string(),
            data_type,
            table_id: None,
            column_id: None,
            pg_type: Some(pg_type),
        };
        let bytes = codec.encode(&ServerMessage::RowDescription {
            columns: vec![
                labeled("small", DataType::Integer, PgType::Int2),
                labeled("n", DataType::Integer, PgType::Int4),
                labeled("code", DataType::Text, PgType::Varchar(Some(10))),
                labeled(
                    "amt",
                    DataType::Numeric {
                        precision: Some(10),
                        scale: 2,
                    },
                    PgType::Numeric {
                        precision: Some(10),
                        scale: 2,
                    },
                ),
            ],
            formats: Vec::new(),
        });

        let mut offset = 5;
        assert_eq!(read_i16(&bytes, &mut offset), 4);
        // The declared width/kind/length is reported: int2/int4 OIDs, varchar with
        // its length typmod (n + 4), and numeric with its packed precision/scale.
        for (name, oid, typlen, typmod) in [
            ("small", 21, 2, -1),
            ("n", 23, 4, -1),
            ("code", 1043, -1, 14),
            ("amt", 1700, -1, ((10 << 16) | 2) + 4),
        ] {
            assert_eq!(read_cstr(&bytes, &mut offset), name);
            assert_eq!(read_i32(&bytes, &mut offset), 0);
            assert_eq!(read_i16(&bytes, &mut offset), 0);
            assert_eq!(read_i32(&bytes, &mut offset), oid);
            assert_eq!(read_i16(&bytes, &mut offset), typlen);
            assert_eq!(read_i32(&bytes, &mut offset), typmod);
            assert_eq!(read_i16(&bytes, &mut offset), 0);
        }
        assert_eq!(offset, bytes.len());
    }

    #[test]
    fn binary_integer_encodes_to_declared_width() {
        // Text encoding is width-independent (just the digits).
        assert_eq!(
            encode_value_with_type(&Value::Integer(1000), &PgType::Int2, 0)
                .unwrap()
                .unwrap(),
            b"1000"
        );
        // Binary encoding honors the declared wire width.
        assert_eq!(
            encode_value_with_type(&Value::Integer(1000), &PgType::Int2, 1)
                .unwrap()
                .unwrap(),
            1000i16.to_be_bytes()
        );
        assert_eq!(
            encode_value_with_type(&Value::Integer(1000), &PgType::Int4, 1)
                .unwrap()
                .unwrap(),
            1000i32.to_be_bytes()
        );
        assert_eq!(
            encode_value_with_type(&Value::Integer(1000), &PgType::Int8, 1)
                .unwrap()
                .unwrap(),
            1000i64.to_be_bytes()
        );
        // A value that does not fit its declared width is rejected, not truncated.
        assert!(encode_value_with_type(&Value::Integer(40000), &PgType::Int2, 1).is_err());
        // A non-integer ignores the wire type for width and delegates to encode_value.
        assert_eq!(
            encode_value_with_type(&Value::Text("hi".to_string()), &PgType::Text, 1)
                .unwrap()
                .unwrap(),
            b"hi"
        );
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

    #[test]
    fn decodes_copy_data_done_and_fail() {
        let mut codec = PostgresCodec::new();
        let mut bytes = tagged(b'd', b"row1\trow2\n");
        bytes.extend_from_slice(&tagged(b'c', &[]));
        bytes.extend_from_slice(&tagged(b'f', b"client aborted\0"));

        assert_eq!(
            codec.decode(&bytes).unwrap(),
            vec![
                ClientMessage::CopyData(b"row1\trow2\n".to_vec()),
                ClientMessage::CopyDone,
                ClientMessage::CopyFail("client aborted".to_string()),
            ]
        );
    }

    #[test]
    fn decodes_empty_copy_data_frame() {
        let mut codec = PostgresCodec::new();
        assert_eq!(
            codec.decode(&tagged(b'd', &[])).unwrap(),
            vec![ClientMessage::CopyData(Vec::new())]
        );
    }

    #[test]
    fn copy_done_with_nonempty_body_is_protocol_error() {
        let mut codec = PostgresCodec::new();
        let err = codec.decode(&tagged(b'c', b"x")).unwrap_err();
        assert_eq!(err.code, common::SqlState::SyntaxError);
    }

    #[test]
    fn encodes_copy_in_and_out_response() {
        let codec = PostgresCodec::new();
        for (message, tag) in [
            (
                ServerMessage::CopyInResponse {
                    overall_format: 0,
                    column_formats: vec![0, 0],
                },
                b'G',
            ),
            (
                ServerMessage::CopyOutResponse {
                    overall_format: 0,
                    column_formats: vec![0, 0],
                },
                b'H',
            ),
        ] {
            let bytes = codec.encode(&message);
            assert_eq!(bytes[0], tag);
            let mut offset = 1;
            let length = read_i32(&bytes, &mut offset);
            assert_eq!(usize::try_from(length).unwrap(), bytes.len() - 1);
            assert_eq!(bytes[offset], 0); // overall_format = text
            offset += 1;
            assert_eq!(read_i16(&bytes, &mut offset), 2); // column count
            assert_eq!(read_i16(&bytes, &mut offset), 0);
            assert_eq!(read_i16(&bytes, &mut offset), 0);
        }
    }

    #[test]
    fn encodes_copy_data_and_done() {
        let codec = PostgresCodec::new();

        let mut expected = vec![b'd'];
        expected.extend_from_slice(&8i32.to_be_bytes()); // length = 4 + 4-byte payload
        expected.extend_from_slice(b"a,b\n");
        assert_eq!(
            codec.encode(&ServerMessage::CopyData(b"a,b\n".to_vec())),
            expected
        );

        assert_eq!(
            codec.encode(&ServerMessage::CopyDone),
            vec![b'c', 0, 0, 0, 4]
        );
    }

    #[test]
    fn copy_message_outside_copy_mode_is_protocol_error() {
        let mut state = PostgresConnectionState::new();
        let err = state
            .handle_message(ClientMessage::CopyData(vec![1, 2, 3]))
            .unwrap_err();
        assert_eq!(err.code, common::SqlState::SyntaxError);
    }
}
