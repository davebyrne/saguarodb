use std::collections::BTreeMap;
use std::sync::Mutex;

use common::{
    ColumnInfo, DataType, DbError, GucSetting, IsolationLevel, POSTGRES_COMPAT_VERSION, Result,
    Row, SessionSequenceState, SqlState, Value,
};
use executor::ExecutionResult;
use parser::{SetScope, Statement};

use super::{QueryService, Transaction, mark_failed_on_error, reset_complete, set_complete};

/// Per-connection accept-all GUC store for driver compatibility.
///
/// PostgreSQL rejects unknown parameters. SaguaroDB deliberately stores arbitrary
/// names so client handshake/introspection statements can run. A few parameters
/// with real server behavior (`transaction_isolation`,
/// `default_transaction_isolation`) are derived from transaction state instead of
/// living in this map.
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
            ("extra_float_digits", "1"),
            ("integer_datetimes", "on"),
            ("search_path", "\"$user\", public"),
            ("server_encoding", "UTF8"),
            ("server_version", POSTGRES_COMPAT_VERSION),
            ("standard_conforming_strings", "on"),
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
            Some(name) => {
                gucs.reset(name);
                (slot, default_isolation, Ok(reset_complete()))
            }
            None => {
                gucs.reset_all();
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
}
