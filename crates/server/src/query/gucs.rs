use std::collections::BTreeMap;
use std::sync::Mutex;

use common::{
    ColumnInfo, DataType, DbError, GucSetting, IsolationLevel, POSTGRES_COMPAT_VERSION, Result,
    Row, SessionSequenceState, SqlState, Value,
};
use executor::ExecutionResult;
use parser::{SetScope, Statement};

use super::{QueryService, Transaction, mark_failed_on_error, reset_complete, set_complete};

const STATEMENT_TIMEOUT: &str = "statement_timeout";
const MAX_STATEMENT_TIMEOUT_MS: u64 = i32::MAX as u64;
const DEFAULT_STATISTICS_TARGET: &str = "default_statistics_target";
/// ANALYZE samples `300 x default_statistics_target` rows
/// (`docs/specs/statistics.md` §6); the range mirrors a bounded slice of
/// PostgreSQL's 1..=10000.
pub(crate) const DEFAULT_STATISTICS_TARGET_DEFAULT: u32 = 100;
const STATISTICS_TARGET_RANGE: std::ops::RangeInclusive<i64> = 1..=1000;

/// Per-connection accept-all GUC store for driver compatibility.
///
/// PostgreSQL rejects unknown parameters. SaguaroDB deliberately stores arbitrary
/// names so client handshake/introspection statements can run. A few parameters
/// with real server behavior are validated before storage. Isolation parameters
/// are derived from transaction state instead of living in this map;
/// `statement_timeout` is stored canonically as integer milliseconds.
#[derive(Debug)]
pub struct SessionGucs {
    defaults: BTreeMap<String, String>,
    settings: Mutex<BTreeMap<String, String>>,
}

impl SessionGucs {
    pub fn new(application_name: String) -> Self {
        let mut defaults = BTreeMap::new();
        for (name, value) in [
            ("application_name", application_name.as_str()),
            ("client_encoding", "UTF8"),
            ("datestyle", "ISO"),
            (DEFAULT_STATISTICS_TARGET, "100"),
            ("extra_float_digits", "1"),
            ("integer_datetimes", "on"),
            ("search_path", "\"$user\", public"),
            ("server_encoding", "UTF8"),
            ("server_version", POSTGRES_COMPAT_VERSION),
            ("standard_conforming_strings", "on"),
            (STATEMENT_TIMEOUT, "0"),
            ("timezone", "UTC"),
        ] {
            defaults.insert(name.to_string(), value.to_string());
        }
        let settings = Mutex::new(defaults.clone());
        Self { defaults, settings }
    }

    pub fn set(&self, name: &str, value: String) {
        self.lock().insert(name.to_string(), value);
    }

    pub fn get(&self, name: &str) -> Option<String> {
        self.lock().get(name).cloned()
    }

    pub fn search_path_names(&self, user: &str) -> Vec<String> {
        self.get("search_path")
            .unwrap_or_else(|| "\"$user\", public".to_string())
            .split(',')
            .filter_map(|entry| {
                let entry = entry.trim();
                if entry.is_empty() {
                    return None;
                }
                let unquoted = entry
                    .strip_prefix('"')
                    .and_then(|entry| entry.strip_suffix('"'))
                    .unwrap_or(entry);
                let name = if unquoted == "$user" { user } else { unquoted };
                (!name.is_empty()).then(|| name.to_ascii_lowercase())
            })
            .collect()
    }

    pub fn reset(&self, name: &str) {
        let mut settings = self.lock();
        match self.defaults.get(name) {
            Some(default) => settings.insert(name.to_string(), default.clone()),
            None => settings.remove(name),
        };
    }

    pub fn reset_all(&self) {
        *self.lock() = self.defaults.clone();
    }

    pub fn all(&self) -> Vec<(String, String)> {
        self.lock()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }

    pub fn settings(
        &self,
        default_isolation: IsolationLevel,
        transaction_isolation: IsolationLevel,
        statement_timeout_ms: u64,
    ) -> Vec<GucSetting> {
        let settings = self.lock().clone();
        let mut rows = settings
            .into_iter()
            .map(|(name, setting)| {
                let boot_val = self.defaults.get(&name).cloned().unwrap_or_default();
                let source = if self.defaults.get(&name) == Some(&setting) {
                    "default"
                } else {
                    "session"
                };
                GucSetting {
                    name,
                    setting,
                    boot_val: boot_val.clone(),
                    reset_val: boot_val,
                    source: source.to_string(),
                }
            })
            .collect::<Vec<_>>();
        if let Some(setting) = rows
            .iter_mut()
            .find(|setting| setting.name == STATEMENT_TIMEOUT)
        {
            setting.setting = statement_timeout_ms.to_string();
            setting.source = if statement_timeout_ms == 0 {
                "default".to_string()
            } else {
                "session".to_string()
            };
        }
        let boot_val = isolation_setting(IsolationLevel::default()).to_string();
        rows.push(GucSetting {
            name: "default_transaction_isolation".to_string(),
            setting: isolation_setting(default_isolation).to_string(),
            boot_val: boot_val.clone(),
            reset_val: boot_val.clone(),
            source: if default_isolation == IsolationLevel::default() {
                "default".to_string()
            } else {
                "session".to_string()
            },
        });
        rows.push(GucSetting {
            name: "transaction_isolation".to_string(),
            setting: isolation_setting(transaction_isolation).to_string(),
            boot_val: boot_val.clone(),
            reset_val: boot_val,
            source: if transaction_isolation == IsolationLevel::default() {
                "default".to_string()
            } else {
                "session".to_string()
            },
        });
        rows.sort_by(|left, right| left.name.cmp(&right.name));
        rows
    }

    pub fn application_name(&self) -> String {
        self.get("application_name").unwrap_or_default()
    }

    pub(crate) fn statement_timeout_ms(&self) -> u64 {
        self.get(STATEMENT_TIMEOUT)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    /// The session's ANALYZE statistics target (`docs/specs/statistics.md`
    /// §6). Values are validated on SET, so the parse fallback only covers a
    /// never-set/foreign value.
    pub(crate) fn default_statistics_target(&self) -> u32 {
        self.get(DEFAULT_STATISTICS_TARGET)
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_STATISTICS_TARGET_DEFAULT)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, String>> {
        self.settings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for SessionGucs {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl QueryService {
    pub(super) fn handle_session_config(
        &self,
        statement: Statement,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        gucs: &SessionGucs,
        session_sequences: &SessionSequenceState,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        if let Some(txn) = &slot
            && txn.failed
        {
            return (slot, default_isolation, Err(failed_transaction_error()));
        }

        match statement {
            Statement::SetVariable { scope, name, value } => {
                self.handle_set_variable(scope, name, value, slot, default_isolation, gucs)
            }
            Statement::ResetVariable { name } => {
                self.handle_reset_variable(name, slot, default_isolation, gucs)
            }
            Statement::ShowVariable { name: Some(name) } => {
                let value = show_value(&name, &slot, default_isolation, gucs);
                match value {
                    Some(value) => (slot, default_isolation, Ok(show_result(&name, value))),
                    None => (
                        mark_failed_on_error(slot),
                        default_isolation,
                        Err(DbError::execute(
                            SqlState::UndefinedObject,
                            format!("unrecognized configuration parameter \"{name}\""),
                        )),
                    ),
                }
            }
            Statement::ShowVariable { name: None } => {
                let result = show_all_result(gucs, &slot, default_isolation);
                (slot, default_isolation, Ok(result))
            }
            Statement::DiscardAll => {
                if let Some(mut txn) = slot {
                    txn.failed = true;
                    return (
                        Some(txn),
                        default_isolation,
                        Err(DbError::plan(
                            SqlState::FeatureNotSupported,
                            "DISCARD ALL cannot run inside a transaction block",
                        )),
                    );
                }
                gucs.reset_all();
                if let Err(err) = session_sequences.reset_all() {
                    return (None, default_isolation, Err(err));
                }
                (
                    None,
                    IsolationLevel::default(),
                    Ok(ExecutionResult::Modified {
                        command: "DISCARD ALL".to_string(),
                        count: 0,
                    }),
                )
            }
            other => (
                slot,
                default_isolation,
                Err(DbError::internal(format!(
                    "handle_session_config received a non-session-config statement: {other:?}"
                ))),
            ),
        }
    }

    fn handle_set_variable(
        &self,
        scope: SetScope,
        name: String,
        value: String,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        gucs: &SessionGucs,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        match name.as_str() {
            "transaction_isolation" => {
                let default = effective_default_isolation(&slot, default_isolation);
                let Some(level) = parse_optional_isolation_setting(&value, default) else {
                    return invalid_isolation(&name, slot, default_isolation, &value);
                };
                let (slot, result) = self.handle_set_transaction(Some(level), slot);
                (slot, default_isolation, result)
            }
            "default_transaction_isolation" => {
                let Some(level) =
                    parse_optional_isolation_setting(&value, IsolationLevel::default())
                else {
                    return invalid_isolation(&name, slot, default_isolation, &value);
                };
                set_default_transaction_isolation(scope, level, slot, default_isolation)
            }
            STATEMENT_TIMEOUT => {
                let timeout_ms = if value.eq_ignore_ascii_case("default") {
                    0
                } else {
                    match parse_statement_timeout_setting(&value) {
                        Ok(timeout_ms) => timeout_ms,
                        Err(err) => {
                            return (mark_failed_on_error(slot), default_isolation, Err(err));
                        }
                    }
                };
                let slot = set_statement_timeout(scope, timeout_ms, slot, gucs);
                (slot, default_isolation, Ok(set_complete()))
            }
            DEFAULT_STATISTICS_TARGET => {
                if value.eq_ignore_ascii_case("default") {
                    gucs.reset(&name);
                    return (slot, default_isolation, Ok(set_complete()));
                }
                match parse_statistics_target_setting(&value) {
                    Ok(target) => {
                        gucs.set(&name, target.to_string());
                        (slot, default_isolation, Ok(set_complete()))
                    }
                    Err(err) => (mark_failed_on_error(slot), default_isolation, Err(err)),
                }
            }
            _ if value.eq_ignore_ascii_case("default") => {
                gucs.reset(&name);
                (slot, default_isolation, Ok(set_complete()))
            }
            _ => {
                gucs.set(&name, value);
                (slot, default_isolation, Ok(set_complete()))
            }
        }
    }

    fn handle_reset_variable(
        &self,
        name: Option<String>,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        gucs: &SessionGucs,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        match name.as_deref() {
            Some("transaction_isolation") => {
                let default = effective_default_isolation(&slot, default_isolation);
                let (slot, result) = self.handle_set_transaction(Some(default), slot);
                (slot, default_isolation, result.map(|_| reset_complete()))
            }
            Some("default_transaction_isolation") => {
                let (slot, default_isolation, result) = set_default_transaction_isolation(
                    SetScope::Session,
                    IsolationLevel::default(),
                    slot,
                    default_isolation,
                );
                (slot, default_isolation, result.map(|_| reset_complete()))
            }
            Some(STATEMENT_TIMEOUT) => {
                let slot = set_statement_timeout(SetScope::Session, 0, slot, gucs);
                (slot, default_isolation, Ok(reset_complete()))
            }
            Some(name) => {
                gucs.reset(name);
                (slot, default_isolation, Ok(reset_complete()))
            }
            None => {
                let session_timeout_ms = gucs.statement_timeout_ms();
                gucs.reset_all();
                if slot.is_some() {
                    gucs.set(STATEMENT_TIMEOUT, session_timeout_ms.to_string());
                }
                let slot = set_statement_timeout(SetScope::Session, 0, slot, gucs);
                let (slot, default_isolation, result) = set_default_transaction_isolation(
                    SetScope::Session,
                    IsolationLevel::default(),
                    slot,
                    default_isolation,
                );
                (slot, default_isolation, result.map(|_| reset_complete()))
            }
        }
    }
}

fn set_statement_timeout(
    scope: SetScope,
    timeout_ms: u64,
    slot: Option<Transaction>,
    gucs: &SessionGucs,
) -> Option<Transaction> {
    match (scope, slot) {
        (SetScope::Local, None) => None,
        (SetScope::Session, None) => {
            gucs.set(STATEMENT_TIMEOUT, timeout_ms.to_string());
            None
        }
        (SetScope::Local, Some(mut txn)) => {
            txn.set_local_statement_timeout(timeout_ms);
            Some(txn)
        }
        (SetScope::Session, Some(mut txn)) => {
            txn.set_statement_timeout(timeout_ms);
            Some(txn)
        }
    }
}

fn effective_statement_timeout_ms(slot: &Option<Transaction>, gucs: &SessionGucs) -> u64 {
    let session_timeout_ms = gucs.statement_timeout_ms();
    slot.as_ref()
        .map(|txn| txn.current_statement_timeout_ms(session_timeout_ms))
        .unwrap_or(session_timeout_ms)
}

fn parse_statistics_target_setting(value: &str) -> Result<u32> {
    let parsed = value.trim().parse::<i64>().map_err(|_| {
        DbError::execute(
            SqlState::InvalidParameterValue,
            format!("invalid value for parameter \"{DEFAULT_STATISTICS_TARGET}\": \"{value}\""),
        )
    })?;
    if !STATISTICS_TARGET_RANGE.contains(&parsed) {
        return Err(DbError::execute(
            SqlState::InvalidParameterValue,
            format!(
                "{parsed} is outside the valid range for parameter \"{DEFAULT_STATISTICS_TARGET}\" ({} .. {})",
                STATISTICS_TARGET_RANGE.start(),
                STATISTICS_TARGET_RANGE.end()
            ),
        ));
    }
    Ok(parsed as u32)
}

fn parse_statement_timeout_setting(value: &str) -> Result<u64> {
    parse_timeout_ms(value).ok_or_else(|| {
        DbError::execute(
            SqlState::InvalidParameterValue,
            format!("invalid value for parameter \"{STATEMENT_TIMEOUT}\": \"{value}\""),
        )
    })
}

fn parse_timeout_ms(value: &str) -> Option<u64> {
    let value = value.trim();
    let (number, remainder) = split_timeout_number(value)?;
    let unit = remainder.trim();
    let numeric_value = parse_postgres_number(number)?;
    if !numeric_value.is_finite() {
        return None;
    }

    let (multiplier, next_smaller) = match unit {
        "" => (1.0, None),
        "d" => (86_400_000.0, Some(3_600_000.0)),
        "h" => (3_600_000.0, Some(60_000.0)),
        "min" => (60_000.0, Some(1_000.0)),
        "s" => (1_000.0, Some(1.0)),
        "ms" => (1.0, Some(0.001)),
        "us" => (0.001, None),
        _ => return None,
    };
    let mut timeout_ms = numeric_value * multiplier;
    if let Some(next_smaller) = next_smaller {
        timeout_ms = (timeout_ms / next_smaller).round_ties_even() * next_smaller;
    }
    timeout_ms = timeout_ms.round_ties_even();
    if !(0.0..=MAX_STATEMENT_TIMEOUT_MS as f64).contains(&timeout_ms) {
        return None;
    }
    Some(timeout_ms as u64)
}

fn split_timeout_number(value: &str) -> Option<(&str, &str)> {
    let bytes = value.as_bytes();
    let mut cursor = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    if bytes.get(cursor) == Some(&b'0') && matches!(bytes.get(cursor + 1), Some(b'x') | Some(b'X'))
    {
        cursor += 2;
        let digits_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_hexdigit) {
            cursor += 1;
        }
        return (cursor > digits_start).then_some((&value[..cursor], &value[cursor..]));
    }

    let digits_start = cursor;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
    }
    let mut has_digits = cursor > digits_start;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        has_digits |= cursor > fraction_start;
    }
    if !has_digits {
        return None;
    }
    if matches!(bytes.get(cursor), Some(b'e') | Some(b'E')) {
        cursor += 1;
        if matches!(bytes.get(cursor), Some(b'+') | Some(b'-')) {
            cursor += 1;
        }
        let exponent_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == exponent_start {
            return None;
        }
    }
    Some((&value[..cursor], &value[cursor..]))
}

fn parse_postgres_number(number: &str) -> Option<f64> {
    let unsigned = number
        .strip_prefix('+')
        .or_else(|| number.strip_prefix('-'))
        .unwrap_or(number);
    let sign = if number.starts_with('-') { -1.0 } else { 1.0 };
    if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        return u64::from_str_radix(hex, 16)
            .ok()
            .map(|value| sign * value as f64);
    }
    if unsigned.len() > 1 && unsigned.starts_with('0') && !unsigned.contains(['.', 'e', 'E']) {
        return u64::from_str_radix(unsigned, 8)
            .ok()
            .map(|value| sign * value as f64);
    }
    number.parse().ok()
}

pub(super) fn display_statement_timeout(timeout_ms: u64) -> String {
    if timeout_ms == 0 {
        return "0".to_string();
    }
    for (unit, divisor) in [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("min", 60_000),
        ("s", 1_000),
    ] {
        if timeout_ms.is_multiple_of(divisor) {
            return format!("{}{unit}", timeout_ms / divisor);
        }
    }
    format!("{timeout_ms}ms")
}

fn set_default_transaction_isolation(
    scope: SetScope,
    level: IsolationLevel,
    slot: Option<Transaction>,
    default_isolation: IsolationLevel,
) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
    match (scope, slot) {
        (SetScope::Local, None) => (None, default_isolation, Ok(set_complete())),
        (SetScope::Session, None) => (None, level, Ok(set_complete())),
        (SetScope::Local, Some(mut txn)) => {
            txn.set_local_default_isolation(level);
            (Some(txn), default_isolation, Ok(set_complete()))
        }
        (SetScope::Session, Some(mut txn)) => {
            txn.set_default_isolation(level);
            (Some(txn), default_isolation, Ok(set_complete()))
        }
    }
}

fn effective_default_isolation(
    slot: &Option<Transaction>,
    session_default: IsolationLevel,
) -> IsolationLevel {
    slot.as_ref()
        .map(|txn| txn.current_default_isolation(session_default))
        .unwrap_or(session_default)
}

fn failed_transaction_error() -> DbError {
    DbError::execute(
        SqlState::InFailedSqlTransaction,
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}

fn invalid_isolation(
    name: &str,
    slot: Option<Transaction>,
    default_isolation: IsolationLevel,
    value: &str,
) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
    (
        mark_failed_on_error(slot),
        default_isolation,
        Err(DbError::execute(
            SqlState::InvalidParameterValue,
            format!("invalid value for parameter \"{name}\": \"{value}\""),
        )),
    )
}

fn parse_optional_isolation_setting(
    value: &str,
    default: IsolationLevel,
) -> Option<IsolationLevel> {
    if value.eq_ignore_ascii_case("default") {
        return Some(default);
    }
    parse_isolation_setting(value)
}

fn parse_isolation_setting(value: &str) -> Option<IsolationLevel> {
    let normalized = value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .replace(['_', '-'], " ")
        .to_ascii_lowercase();
    match normalized.split_whitespace().collect::<Vec<_>>().as_slice() {
        ["read", "uncommitted"] | ["read", "committed"] => Some(IsolationLevel::ReadCommitted),
        ["repeatable", "read"] => Some(IsolationLevel::RepeatableRead),
        ["serializable"] => Some(IsolationLevel::Serializable),
        _ => None,
    }
}

fn show_value(
    name: &str,
    slot: &Option<Transaction>,
    default_isolation: IsolationLevel,
    gucs: &SessionGucs,
) -> Option<String> {
    match name {
        "transaction_isolation" => {
            let level = slot
                .as_ref()
                .map(|txn| txn.isolation)
                .unwrap_or(default_isolation);
            Some(isolation_setting(level).to_string())
        }
        "default_transaction_isolation" => Some(
            isolation_setting(effective_default_isolation(slot, default_isolation)).to_string(),
        ),
        STATEMENT_TIMEOUT => Some(display_statement_timeout(effective_statement_timeout_ms(
            slot, gucs,
        ))),
        _ => gucs.get(name),
    }
}

fn isolation_setting(level: IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

fn text_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type: DataType::Text,
        table_id: None,
        column_id: None,
        pg_type: None,
    }
}

fn show_result(name: &str, value: String) -> ExecutionResult {
    ExecutionResult::ModifiedReturning {
        command: "SHOW".to_string(),
        count: 0,
        columns: vec![text_column(name)],
        rows: vec![Row {
            values: vec![Value::Text(value)],
        }],
    }
}

fn show_all_result(
    gucs: &SessionGucs,
    slot: &Option<Transaction>,
    default_isolation: IsolationLevel,
) -> ExecutionResult {
    let mut settings = gucs.all();
    if let Some((_, setting)) = settings
        .iter_mut()
        .find(|(name, _)| name == STATEMENT_TIMEOUT)
    {
        *setting = display_statement_timeout(effective_statement_timeout_ms(slot, gucs));
    }
    settings.push((
        "default_transaction_isolation".to_string(),
        isolation_setting(effective_default_isolation(slot, default_isolation)).to_string(),
    ));
    settings.push((
        "transaction_isolation".to_string(),
        isolation_setting(
            slot.as_ref()
                .map(|txn| txn.isolation)
                .unwrap_or(default_isolation),
        )
        .to_string(),
    ));
    settings.sort();
    let rows = settings
        .into_iter()
        .map(|(name, setting)| Row {
            values: vec![
                Value::Text(name),
                Value::Text(setting),
                Value::Text(String::new()),
            ],
        })
        .collect();
    ExecutionResult::ModifiedReturning {
        command: "SHOW".to_string(),
        count: 0,
        columns: vec![
            text_column("name"),
            text_column("setting"),
            text_column("description"),
        ],
        rows,
    }
}

pub(super) fn session_config_result_columns(statement: &Statement) -> Option<Vec<ColumnInfo>> {
    match statement {
        Statement::ShowVariable { name: Some(name) } => Some(vec![text_column(name)]),
        Statement::ShowVariable { name: None } => Some(vec![
            text_column("name"),
            text_column("setting"),
            text_column("description"),
        ]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guc_store_resets_unknown_values_by_removing_them() {
        let gucs = SessionGucs::default();
        gucs.set("my_app.batch_size", "250".to_string());
        assert_eq!(gucs.get("my_app.batch_size").as_deref(), Some("250"));

        gucs.reset("my_app.batch_size");
        assert_eq!(gucs.get("my_app.batch_size"), None);
    }

    #[test]
    fn search_path_names_expand_user_and_normalize_entries() {
        let gucs = SessionGucs::default();
        assert_eq!(gucs.search_path_names("Alice"), ["alice", "public"]);

        gucs.set("search_path", "app, Reporting".to_string());
        assert_eq!(gucs.search_path_names("Alice"), ["app", "reporting"]);
    }

    #[test]
    fn isolation_settings_match_postgres_spellings() {
        assert_eq!(
            parse_isolation_setting("read uncommitted"),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            parse_isolation_setting("repeatable_read"),
            Some(IsolationLevel::RepeatableRead)
        );
        assert_eq!(
            parse_isolation_setting("SERIALIZABLE"),
            Some(IsolationLevel::Serializable)
        );
        assert_eq!(parse_isolation_setting("snapshot"), None);
        assert_eq!(parse_isolation_setting("bogus"), None);
    }

    #[test]
    fn statement_timeout_values_are_parsed_as_canonical_milliseconds() {
        for (value, expected) in [
            ("0", 0),
            ("1.5", 2),
            ("2.5", 2),
            ("1e3", 1_000),
            ("010", 8),
            ("0x10", 16),
            ("250", 250),
            ("250ms", 250),
            ("250 ms", 250),
            ("1500us", 2),
            ("0.5 s", 500),
            ("1.5min", 90_000),
            ("1h", 3_600_000),
            ("0.5d", 43_200_000),
            ("+1 s", 1_000),
            ("-0.5", 0),
            ("2147483647", MAX_STATEMENT_TIMEOUT_MS),
        ] {
            assert_eq!(parse_timeout_ms(value), Some(expected), "value: {value}");
        }
    }

    #[test]
    fn statement_timeout_rejects_invalid_or_out_of_range_values() {
        for value in [
            "",
            "-1",
            "08",
            "0x",
            "1e",
            "1 sec",
            "1MS",
            "NaN",
            "infinity",
            "2147483648",
            "1000000000000000000000000000000000000000d",
        ] {
            let err = parse_statement_timeout_setting(value).unwrap_err();
            assert_eq!(err.code, SqlState::InvalidParameterValue, "value: {value}");
        }
    }

    #[test]
    fn statement_timeout_defaults_to_disabled_and_is_stored_canonically() {
        let gucs = SessionGucs::default();
        assert_eq!(gucs.get(STATEMENT_TIMEOUT).as_deref(), Some("0"));

        let timeout_ms = parse_statement_timeout_setting("1.5 s").unwrap();
        gucs.set(STATEMENT_TIMEOUT, timeout_ms.to_string());
        assert_eq!(gucs.get(STATEMENT_TIMEOUT).as_deref(), Some("1500"));

        gucs.reset(STATEMENT_TIMEOUT);
        assert_eq!(gucs.get(STATEMENT_TIMEOUT).as_deref(), Some("0"));
    }

    #[test]
    fn statement_timeout_display_uses_the_largest_exact_time_unit() {
        assert_eq!(display_statement_timeout(0), "0");
        assert_eq!(display_statement_timeout(999), "999ms");
        assert_eq!(display_statement_timeout(1_000), "1s");
        assert_eq!(display_statement_timeout(1_500), "1500ms");
        assert_eq!(display_statement_timeout(120_000), "2min");
        assert_eq!(display_statement_timeout(86_400_000), "1d");
    }
}
