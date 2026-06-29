//! Pure text/CSV format engine for `COPY` (see `docs/specs/copy.md` §3).
//!
//! [`CopyParser`] turns a byte stream of `COPY ... FROM STDIN` data into rows of
//! [`Value`]s, buffering a partial trailing record across chunks (CSV records may
//! span newlines inside quoted fields). [`format_row`]/[`format_header`] render
//! rows for `COPY ... TO STDOUT`. Nothing here touches storage or IO — the
//! executor's COPY routines drive it.

use common::{CopyFormat, CopyOptions, DataType, DbError, Result, SqlState, Value};

fn malformed(message: impl Into<String>) -> DbError {
    DbError::execute(SqlState::BadCopyFileFormat, message)
}

fn invalid_value(message: impl Into<String>) -> DbError {
    DbError::execute(SqlState::InvalidTextRepresentation, message)
}

/// Streaming parser for `COPY ... FROM STDIN` data.
pub struct CopyParser {
    options: CopyOptions,
    /// Column types in COPY order; a parsed row has exactly this many fields.
    column_types: Vec<DataType>,
    buffer: Vec<u8>,
    /// When the COPY has `HEADER`, the first record is the header line and is
    /// skipped (no `MATCH` validation).
    skip_header: bool,
}

impl CopyParser {
    pub fn new(column_types: Vec<DataType>, options: CopyOptions) -> Self {
        let skip_header = options.header;
        Self {
            options,
            column_types,
            buffer: Vec::new(),
            skip_header,
        }
    }

    /// Feed a chunk and return every row that is now complete.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Vec<Value>>> {
        self.buffer.extend_from_slice(chunk);
        self.extract(false)
    }

    /// End of input: flush a non-empty trailing record (a final row need not end
    /// with a newline). Errors if leftover bytes are not valid UTF-8 or a CSV
    /// quoted field is left open.
    pub fn finish(&mut self) -> Result<Vec<Vec<Value>>> {
        self.extract(true)
    }

    fn extract(&mut self, at_eof: bool) -> Result<Vec<Vec<Value>>> {
        // Only the longest valid-UTF-8 prefix is parseable; a multi-byte character
        // split across a chunk boundary stays buffered until the rest of its bytes
        // arrive. Record terminators (`\n`) and the option characters are ASCII, so
        // this never hides a complete record. Validate and decode once per call,
        // then advance a cursor through the prefix so a chunk with R rows and B
        // bytes costs O(B), not O(R × B).
        let valid_len = match std::str::from_utf8(&self.buffer) {
            Ok(text) => text.len(),
            Err(err) => err.valid_up_to(),
        };
        let text = std::str::from_utf8(&self.buffer[..valid_len])
            .expect("valid_up_to bytes are valid UTF-8");

        let mut rows = Vec::new();
        let mut cursor = 0;
        loop {
            let remaining = &text[cursor..];
            let record = match self.options.format {
                CopyFormat::Text => next_text_record(remaining, &self.options, at_eof)?,
                CopyFormat::Csv => next_csv_record(remaining, &self.options, at_eof)?,
            };
            let Some((fields, consumed)) = record else {
                break;
            };
            cursor += consumed;

            if self.skip_header {
                self.skip_header = false;
                continue;
            }
            rows.push(fields_to_row(fields, &self.column_types)?);
        }
        self.buffer.drain(..cursor);

        if at_eof && !self.buffer.is_empty() {
            return Err(invalid_value("COPY data is not valid UTF-8"));
        }
        Ok(rows)
    }
}

/// Convert parsed fields (`None` = NULL) to a typed row, enforcing arity.
fn fields_to_row(fields: Vec<Option<String>>, column_types: &[DataType]) -> Result<Vec<Value>> {
    if fields.len() != column_types.len() {
        return Err(malformed(format!(
            "COPY row has {} fields, expected {}",
            fields.len(),
            column_types.len()
        )));
    }
    fields
        .into_iter()
        .zip(column_types)
        .map(|(field, data_type)| match field {
            None => Ok(Value::Null),
            Some(text) => parse_field(&text, data_type),
        })
        .collect()
}

fn parse_field(text: &str, data_type: &DataType) -> Result<Value> {
    match data_type {
        DataType::Integer => text
            .trim()
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| invalid_value(format!("invalid input syntax for integer: \"{text}\""))),
        DataType::Boolean => common::parse_bool_text(text)
            .map(Value::Boolean)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for boolean: \"{text}\""))),
        DataType::Text => Ok(Value::Text(text.to_string())),
        DataType::Date => common::datetime::parse_date(text)
            .map(Value::Date)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for date: \"{text}\""))),
        DataType::Timestamp => common::datetime::parse_timestamp(text)
            .map(Value::Timestamp)
            .ok_or_else(|| {
                invalid_value(format!("invalid input syntax for timestamp: \"{text}\""))
            }),
        DataType::Bytea => common::bytea::parse_hex(text)
            .map(Value::Bytes)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for bytea: \"{text}\""))),
        DataType::Uuid => common::uuid::parse_uuid(text)
            .map(Value::Uuid)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for uuid: \"{text}\""))),
        DataType::Double => common::float::parse_double(text)
            .map(|value| Value::Float(value.into()))
            .ok_or_else(|| {
                invalid_value(format!(
                    "invalid input syntax for double precision: \"{text}\""
                ))
            }),
        DataType::Numeric { .. } => common::numeric::parse_numeric(text)
            .map(Value::Numeric)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for numeric: \"{text}\""))),
        DataType::Real => common::float::parse_real(text)
            .map(|value| Value::Real(value.into()))
            .ok_or_else(|| invalid_value(format!("invalid input syntax for real: \"{text}\""))),
        DataType::Time => common::datetime::parse_time(text)
            .map(Value::Time)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for time: \"{text}\""))),
        DataType::TimestampTz => common::datetime::parse_timestamptz(text)
            .map(Value::TimestampTz)
            .ok_or_else(|| {
                invalid_value(format!("invalid input syntax for timestamptz: \"{text}\""))
            }),
        DataType::Interval => common::interval::parse_interval(text)
            .map(Value::Interval)
            .ok_or_else(|| invalid_value(format!("invalid input syntax for interval: \"{text}\""))),
    }
}

// ---- text format ----

/// Take the next `\n`-terminated text record, or (at EOF) the non-empty leftover.
/// A raw `\n` always ends a text record (an in-field newline is the escape `\n`).
fn next_text_record(
    text: &str,
    options: &CopyOptions,
    at_eof: bool,
) -> Result<Option<(Vec<Option<String>>, usize)>> {
    if let Some(newline) = text.find('\n') {
        let mut line = &text[..newline];
        if let Some(stripped) = line.strip_suffix('\r') {
            line = stripped;
        }
        Ok(Some((parse_text_fields(line, options), newline + 1)))
    } else if at_eof && !text.is_empty() {
        let line = text.strip_suffix('\r').unwrap_or(text);
        Ok(Some((parse_text_fields(line, options), text.len())))
    } else {
        Ok(None)
    }
}

/// Split a text-format line into fields on unescaped delimiters. NULL is decided
/// by comparing the *raw* (un-de-escaped) token to the NULL string; non-NULL
/// tokens are then de-escaped.
fn parse_text_fields(line: &str, options: &CopyOptions) -> Vec<Option<String>> {
    let delimiter = options.delimiter;
    let mut fields = Vec::new();
    let mut raw = String::new();
    let mut chars = line.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            raw.push('\\');
            if let Some(escaped) = chars.next() {
                raw.push(escaped);
            }
        } else if ch == delimiter {
            fields.push(text_token(&raw, options));
            raw.clear();
        } else {
            raw.push(ch);
        }
    }
    fields.push(text_token(&raw, options));
    fields
}

fn text_token(raw: &str, options: &CopyOptions) -> Option<String> {
    if raw == options.null_string {
        None
    } else {
        Some(unescape_text(raw))
    }
}

fn unescape_text(token: &str) -> String {
    let mut out = String::with_capacity(token.len());
    let mut chars = token.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('b') => out.push('\u{08}'),
                Some('f') => out.push('\u{0C}'),
                Some('v') => out.push('\u{0B}'),
                // Unrecognized escape decodes to the literal character.
                Some(other) => out.push(other),
                // A trailing lone backslash is a literal backslash.
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

// ---- csv format ----

/// Find the end of the next CSV record (the first `\n` outside a quoted field).
/// Returns `(content, consumed)` where `content` excludes the terminator and a
/// trailing `\r`. `None` while incomplete; errors only at EOF with an open quote.
fn next_csv_record(
    text: &str,
    options: &CopyOptions,
    at_eof: bool,
) -> Result<Option<(Vec<Option<String>>, usize)>> {
    let quote = options.quote;
    let escape = options.escape;
    let mut in_quote = false;
    let mut chars = text.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        if in_quote {
            if ch == quote {
                if escape == quote && matches!(chars.peek(), Some(&(_, next)) if next == quote) {
                    chars.next(); // doubled quote → literal, stay quoted
                } else {
                    in_quote = false; // closing quote
                }
            } else if ch == escape && escape != quote {
                chars.next(); // escaped character
            }
            // any other char (including `\n`) is data inside the quoted field
        } else if ch == quote {
            in_quote = true;
        } else if ch == '\n' {
            let mut content_end = index;
            if content_end > 0 && text.as_bytes()[content_end - 1] == b'\r' {
                content_end -= 1;
            }
            let fields = parse_csv_fields(&text[..content_end], options)?;
            return Ok(Some((fields, index + 1)));
        }
    }

    if at_eof && !text.is_empty() {
        if in_quote {
            return Err(malformed("unterminated CSV quoted field"));
        }
        let content = text.strip_suffix('\r').unwrap_or(text);
        Ok(Some((parse_csv_fields(content, options)?, text.len())))
    } else {
        Ok(None)
    }
}

/// Split one complete CSV record's content (no terminator) into fields. An
/// unquoted field equal to the NULL string is NULL; a quoted field is never NULL.
fn parse_csv_fields(content: &str, options: &CopyOptions) -> Result<Vec<Option<String>>> {
    let quote = options.quote;
    let escape = options.escape;
    let delimiter = options.delimiter;
    let mut fields = Vec::new();
    let mut chars = content.chars().peekable();

    loop {
        if matches!(chars.peek(), Some(&first) if first == quote) {
            chars.next(); // opening quote
            let mut value = String::new();
            loop {
                match chars.next() {
                    None => return Err(malformed("unterminated CSV quoted field")),
                    Some(ch) if ch == quote => {
                        if escape == quote && matches!(chars.peek(), Some(&next) if next == quote) {
                            chars.next();
                            value.push(quote);
                        } else {
                            break; // closing quote
                        }
                    }
                    Some(ch) if ch == escape => match chars.next() {
                        // escape != quote here (the quote case is handled above)
                        Some(next) => value.push(next),
                        None => return Err(malformed("unterminated CSV escape")),
                    },
                    Some(ch) => value.push(ch),
                }
            }
            fields.push(Some(value));
            match chars.next() {
                None => break,
                Some(ch) if ch == delimiter => continue,
                Some(_) => return Err(malformed("unexpected data after CSV closing quote")),
            }
        } else {
            let mut raw = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == delimiter {
                    break;
                }
                raw.push(ch);
                chars.next();
            }
            fields.push(if raw == options.null_string {
                None
            } else {
                Some(raw)
            });
            match chars.next() {
                None => break,
                Some(ch) if ch == delimiter => continue,
                Some(_) => unreachable!("unquoted scan stops only at the delimiter or end"),
            }
        }
    }
    Ok(fields)
}

// ---- output ----

/// Render one row for `COPY ... TO STDOUT`, terminated by `\n`.
pub fn format_row(values: &[Value], options: &CopyOptions) -> Vec<u8> {
    let mut line = String::new();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            line.push(options.delimiter);
        }
        match value_text(value) {
            None => line.push_str(&options.null_string),
            Some(text) => line.push_str(&format_field(&text, options)),
        }
    }
    line.push('\n');
    line.into_bytes()
}

/// Render the `HEADER` line (column names) for `COPY ... TO STDOUT`.
pub fn format_header(names: &[&str], options: &CopyOptions) -> Vec<u8> {
    let mut line = String::new();
    for (index, name) in names.iter().enumerate() {
        if index > 0 {
            line.push(options.delimiter);
        }
        line.push_str(&format_field(name, options));
    }
    line.push('\n');
    line.into_bytes()
}

/// The on-wire string of a value, or `None` for NULL (emitted as the NULL string).
fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Integer(int) => Some(int.to_string()),
        Value::Boolean(flag) => Some(if *flag { "t" } else { "f" }.to_string()),
        Value::Text(text) => Some(text.clone()),
        Value::Date(days) => Some(common::datetime::format_date(*days)),
        Value::Timestamp(micros) => Some(common::datetime::format_timestamp(*micros)),
        Value::Bytes(raw) => Some(common::bytea::format_hex(raw)),
        Value::Uuid(raw) => Some(common::uuid::format_uuid(raw)),
        Value::Float(value) => Some(common::float::format_double(value.0)),
        Value::Numeric(value) => Some(common::numeric::format_numeric(value)),
        Value::Real(value) => Some(common::float::format_real(value.0)),
        Value::Time(micros) => Some(common::datetime::format_time(*micros)),
        Value::TimestampTz(micros) => Some(common::datetime::format_timestamptz(*micros)),
        Value::Interval(iv) => Some(common::interval::format_interval(iv)),
    }
}

fn format_field(value: &str, options: &CopyOptions) -> String {
    match options.format {
        CopyFormat::Text => escape_text_field(value, options.delimiter),
        CopyFormat::Csv => csv_field(value, options),
    }
}

fn escape_text_field(value: &str, delimiter: char) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch == delimiter => {
                out.push('\\');
                out.push(ch);
            }
            ch => out.push(ch),
        }
    }
    out
}

fn csv_field(value: &str, options: &CopyOptions) -> String {
    let needs_quote = value == options.null_string
        || value.contains(options.delimiter)
        || value.contains(options.quote)
        || value.contains('\n')
        || value.contains('\r');
    if !needs_quote {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push(options.quote);
    for ch in value.chars() {
        if ch == options.quote || ch == options.escape {
            out.push(options.escape);
        }
        out.push(ch);
    }
    out.push(options.quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_opts() -> CopyOptions {
        CopyOptions::defaults_for(CopyFormat::Text)
    }

    fn csv_opts() -> CopyOptions {
        CopyOptions::defaults_for(CopyFormat::Csv)
    }

    fn parse_all(
        input: &[u8],
        types: Vec<DataType>,
        options: CopyOptions,
    ) -> Result<Vec<Vec<Value>>> {
        let mut parser = CopyParser::new(types, options);
        let mut rows = parser.push(input)?;
        rows.extend(parser.finish()?);
        Ok(rows)
    }

    fn int_text() -> Vec<DataType> {
        vec![DataType::Integer, DataType::Text]
    }

    #[test]
    fn parses_text_rows_with_null_sentinel() {
        let rows = parse_all(b"1\tann\n2\t\\N\n", int_text(), text_opts()).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Text("ann".to_string())],
                vec![Value::Integer(2), Value::Null],
            ]
        );
    }

    #[test]
    fn parses_text_final_row_without_newline_and_crlf() {
        assert_eq!(
            parse_all(b"1\tann", int_text(), text_opts()).unwrap(),
            vec![vec![Value::Integer(1), Value::Text("ann".to_string())]]
        );
        assert_eq!(
            parse_all(b"1\tann\r\n", int_text(), text_opts()).unwrap(),
            vec![vec![Value::Integer(1), Value::Text("ann".to_string())]]
        );
    }

    #[test]
    fn text_escapes_round_trip() {
        let value = Value::Text("a\tb\nc\\d".to_string());
        let wire = format_row(std::slice::from_ref(&value), &text_opts());
        // Tab, newline, and backslash are all escaped on output.
        assert_eq!(wire, b"a\\tb\\nc\\\\d\n");
        let rows = parse_all(&wire, vec![DataType::Text], text_opts()).unwrap();
        assert_eq!(rows, vec![vec![value]]);
    }

    #[test]
    fn text_literal_backslash_n_is_not_null() {
        // A Text value of `\N` must round-trip as data, not as the NULL sentinel.
        let value = Value::Text("\\N".to_string());
        let wire = format_row(std::slice::from_ref(&value), &text_opts());
        assert_eq!(wire, b"\\\\N\n");
        let rows = parse_all(&wire, vec![DataType::Text], text_opts()).unwrap();
        assert_eq!(rows, vec![vec![value]]);
    }

    #[test]
    fn text_null_output_uses_sentinel() {
        assert_eq!(format_row(&[Value::Null], &text_opts()), b"\\N\n");
    }

    #[test]
    fn parses_csv_rows_with_unquoted_empty_as_null() {
        let rows = parse_all(b"1,ann\n2,\n", int_text(), csv_opts()).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Text("ann".to_string())],
                vec![Value::Integer(2), Value::Null],
            ]
        );
    }

    #[test]
    fn csv_quoted_empty_is_not_null() {
        let rows = parse_all(b"1,\"\"\n", int_text(), csv_opts()).unwrap();
        assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Text(String::new())]]
        );
    }

    #[test]
    fn csv_quoting_delimiter_quote_and_newline() {
        let rows = parse_all(
            b"1,\"a,b\"\n2,\"a\"\"b\"\n3,\"line1\nline2\"\n",
            int_text(),
            csv_opts(),
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Text("a,b".to_string())],
                vec![Value::Integer(2), Value::Text("a\"b".to_string())],
                vec![Value::Integer(3), Value::Text("line1\nline2".to_string())],
            ]
        );
    }

    #[test]
    fn csv_output_quotes_specials_and_doubles_quotes() {
        let row = vec![Value::Integer(1), Value::Text("a,\"b\"\nc".to_string())];
        let wire = format_row(&row, &csv_opts());
        assert_eq!(wire, b"1,\"a,\"\"b\"\"\nc\"\n");
        // Round-trips back to the same row.
        let rows = parse_all(&wire, int_text(), csv_opts()).unwrap();
        assert_eq!(rows, vec![row]);
    }

    #[test]
    fn csv_output_quotes_value_equal_to_null_string() {
        // Default CSV NULL is the empty string, so a real empty Text value is
        // quoted to stay distinct from NULL.
        assert_eq!(
            format_row(&[Value::Text(String::new())], &csv_opts()),
            b"\"\"\n"
        );
        assert_eq!(format_row(&[Value::Null], &csv_opts()), b"\n");
    }

    #[test]
    fn header_is_skipped_on_input() {
        let mut options = csv_opts();
        options.header = true;
        let rows = parse_all(b"id,name\n1,ann\n", int_text(), options).unwrap();
        assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Text("ann".to_string())]]
        );
    }

    #[test]
    fn format_header_renders_column_names() {
        assert_eq!(format_header(&["id", "name"], &csv_opts()), b"id,name\n");
        assert_eq!(format_header(&["id", "name"], &text_opts()), b"id\tname\n");
    }

    #[test]
    fn streams_text_rows_split_across_chunks() {
        let mut parser = CopyParser::new(int_text(), text_opts());
        assert!(parser.push(b"1\tan").unwrap().is_empty());
        let rows = parser.push(b"n\n2\tbob\n").unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Text("ann".to_string())],
                vec![Value::Integer(2), Value::Text("bob".to_string())],
            ]
        );
        assert!(parser.finish().unwrap().is_empty());
    }

    #[test]
    fn streams_csv_embedded_newline_across_chunks() {
        let mut parser = CopyParser::new(int_text(), csv_opts());
        assert!(parser.push(b"1,\"li").unwrap().is_empty());
        let rows = parser.push(b"ne\nrest\"\n").unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Integer(1),
                Value::Text("line\nrest".to_string())
            ]]
        );
    }

    #[test]
    fn streams_multibyte_char_split_across_chunks() {
        // The 'é' (2 UTF-8 bytes) is split across the two pushes.
        let bytes = "1,café\n".as_bytes();
        let split = bytes.iter().position(|&b| b == b'f').unwrap() + 1;
        let mut parser = CopyParser::new(int_text(), csv_opts());
        let mut rows = parser.push(&bytes[..split + 1]).unwrap(); // includes only first byte of 'é'
        rows.extend(parser.push(&bytes[split + 1..]).unwrap());
        rows.extend(parser.finish().unwrap());
        assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Text("café".to_string())]]
        );
    }

    #[test]
    fn parses_boolean_forms() {
        let rows = parse_all(b"t\nyes\n0\n", vec![DataType::Boolean], text_opts()).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Boolean(true)],
                vec![Value::Boolean(true)],
                vec![Value::Boolean(false)],
            ]
        );
    }

    #[test]
    fn rejects_bad_integer_value() {
        let err = parse_all(b"x\tann\n", int_text(), text_opts()).unwrap_err();
        assert_eq!(err.code, SqlState::InvalidTextRepresentation);
    }

    #[test]
    fn rejects_wrong_column_count() {
        let err = parse_all(b"1\n", int_text(), text_opts()).unwrap_err();
        assert_eq!(err.code, SqlState::BadCopyFileFormat);
    }

    #[test]
    fn rejects_unterminated_csv_quote_at_eof() {
        let err = parse_all(b"1,\"abc", int_text(), csv_opts()).unwrap_err();
        assert_eq!(err.code, SqlState::BadCopyFileFormat);
    }

    #[test]
    fn empty_input_yields_no_rows() {
        assert!(parse_all(b"", int_text(), text_opts()).unwrap().is_empty());
        assert!(parse_all(b"", int_text(), csv_opts()).unwrap().is_empty());
    }

    #[test]
    fn csv_custom_escape_round_trips() {
        // ESCAPE distinct from QUOTE: a literal quote is `\"`, a literal backslash
        // is `\\`.
        let mut options = csv_opts();
        options.escape = '\\';
        let value = Value::Text("a\"b\\c".to_string());
        let wire = format_row(std::slice::from_ref(&value), &options);
        assert_eq!(wire, b"\"a\\\"b\\\\c\"\n");
        let rows = parse_all(&wire, vec![DataType::Text], options).unwrap();
        assert_eq!(rows, vec![vec![value]]);
    }

    #[test]
    fn csv_strips_crlf_line_endings() {
        let rows = parse_all(b"1,ann\r\n2,\"bob\"\r\n", int_text(), csv_opts()).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Text("ann".to_string())],
                vec![Value::Integer(2), Value::Text("bob".to_string())],
            ]
        );
    }

    #[test]
    fn rejects_data_after_csv_closing_quote() {
        let err = parse_all(b"1,\"a\"b\n", int_text(), csv_opts()).unwrap_err();
        assert_eq!(err.code, SqlState::BadCopyFileFormat);
    }

    #[test]
    fn streams_many_rows_in_one_chunk() {
        // Exercises the single-pass cursor: many records in one push.
        let mut input = Vec::new();
        for i in 0..500 {
            input.extend_from_slice(format!("{i}\tn{i}\n").as_bytes());
        }
        let rows = parse_all(&input, int_text(), text_opts()).unwrap();
        assert_eq!(rows.len(), 500);
        assert_eq!(rows[499][0], Value::Integer(499));
    }
}
