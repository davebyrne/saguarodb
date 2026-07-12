use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub data_dir: PathBuf,
    pub port: u16,
    pub buffer_pool_frames: usize,
    pub checkpoint_every_n_commits: u64,
    pub checkpoint_wal_bytes: u64,
    /// Auto-prune threshold: when a checkpoint runs and at least this many dead
    /// versions have accumulated since the last auto-prune, the checkpoint runs a
    /// VACUUM pass over every user table under its exclusive guard before flushing
    /// dirty pages (`docs/specs/mvcc.md` §9, Milestone F4b). `0` disables
    /// auto-prune entirely (space is then bounded only by explicit `VACUUM`).
    pub auto_vacuum_dead_rows: u64,
    /// Checkpoint auto-analyze threshold (`docs/specs/statistics.md` §10):
    /// committed changed rows since the last auto-analyze. `0` disables.
    pub auto_analyze_changed_rows: u64,
    pub shutdown_timeout_ms: u64,
    /// How long a writer blocked on an in-progress row-lock holder waits before the
    /// deadlock detector runs a wait-for-graph cycle check (`docs/specs/deadlock.md`).
    /// Matches PostgreSQL's `deadlock_timeout`. Must be positive.
    pub deadlock_timeout_ms: u64,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            port: 5433,
            buffer_pool_frames: 1024,
            checkpoint_every_n_commits: 100,
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            auto_vacuum_dead_rows: 10000,
            auto_analyze_changed_rows: 10000,
            shutdown_timeout_ms: 30000,
            deadlock_timeout_ms: 1000,
            tls_cert_file: None,
            tls_key_file: None,
        }
    }
}

impl Config {
    /// Resolve the configured TLS material. Returns `Ok(Some((cert, key)))` when
    /// TLS is enabled, `Ok(None)` when it is disabled, and `Err` when exactly one
    /// of the cert/key paths is set (TLS needs both or neither). This is the
    /// single source of truth for the both-or-neither rule, used by both CLI
    /// parsing and server startup.
    pub fn tls_files(&self) -> std::result::Result<Option<(&Path, &Path)>, String> {
        match (&self.tls_cert_file, &self.tls_key_file) {
            (Some(cert), Some(key)) => Ok(Some((cert, key))),
            (None, None) => Ok(None),
            (Some(_), None) => Err("--tls-cert-file requires --tls-key-file".to_string()),
            (None, Some(_)) => Err("--tls-key-file requires --tls-cert-file".to_string()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigAction {
    Run(Config),
    Help,
}

pub fn parse_args<I, S>(args: I) -> std::result::Result<ConfigAction, String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut config = Config::default();
    let mut args = args.into_iter().map(Into::into);
    let _program = args.next();

    while let Some(arg) = args.next() {
        let arg = arg
            .into_string()
            .map_err(|_| "arguments must be valid UTF-8".to_string())?;
        match arg.as_str() {
            "--help" => return Ok(ConfigAction::Help),
            "--data-dir" => {
                let value = next_value(&mut args, "--data-dir")?;
                config.data_dir = PathBuf::from(value);
            }
            "--port" => {
                let value = next_value(&mut args, "--port")?;
                config.port = parse_port(&value)?;
            }
            "--buffer-pool-frames" => {
                let value = next_value(&mut args, "--buffer-pool-frames")?;
                config.buffer_pool_frames = parse_positive_usize(&value, "--buffer-pool-frames")?;
            }
            "--checkpoint-every-n-commits" => {
                let value = next_value(&mut args, "--checkpoint-every-n-commits")?;
                config.checkpoint_every_n_commits =
                    parse_positive_u64(&value, "--checkpoint-every-n-commits")?;
            }
            "--checkpoint-wal-bytes" => {
                let value = next_value(&mut args, "--checkpoint-wal-bytes")?;
                config.checkpoint_wal_bytes = parse_positive_u64(&value, "--checkpoint-wal-bytes")?;
            }
            "--auto-vacuum-dead-rows" => {
                // 0 is allowed and disables auto-prune (see Config::auto_vacuum_dead_rows).
                let value = next_value(&mut args, "--auto-vacuum-dead-rows")?;
                config.auto_vacuum_dead_rows = parse_u64(&value, "--auto-vacuum-dead-rows")?;
            }
            "--auto-analyze-changed-rows" => {
                // 0 is allowed and disables auto-analyze
                // (see Config::auto_analyze_changed_rows).
                let value = next_value(&mut args, "--auto-analyze-changed-rows")?;
                config.auto_analyze_changed_rows =
                    parse_u64(&value, "--auto-analyze-changed-rows")?;
            }
            "--shutdown-timeout-ms" => {
                let value = next_value(&mut args, "--shutdown-timeout-ms")?;
                config.shutdown_timeout_ms = parse_positive_u64(&value, "--shutdown-timeout-ms")?;
            }
            "--deadlock-timeout-ms" => {
                let value = next_value(&mut args, "--deadlock-timeout-ms")?;
                config.deadlock_timeout_ms = parse_positive_u64(&value, "--deadlock-timeout-ms")?;
            }
            "--tls-cert-file" => {
                let value = next_value(&mut args, "--tls-cert-file")?;
                config.tls_cert_file = Some(PathBuf::from(value));
            }
            "--tls-key-file" => {
                let value = next_value(&mut args, "--tls-key-file")?;
                config.tls_key_file = Some(PathBuf::from(value));
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    config.tls_files()?;

    Ok(ConfigAction::Run(config))
}

pub fn usage(program: &str) -> String {
    format!(
        "Usage: {program} [OPTIONS]\n\
         \n\
         Options:\n\
           --data-dir <PATH>                  default ./data\n\
           --port <PORT>                      default 5433\n\
           --buffer-pool-frames <N>           default 1024\n\
           --checkpoint-every-n-commits <N>   default 100\n\
           --checkpoint-wal-bytes <BYTES>     default 67108864\n\
           --auto-vacuum-dead-rows <N>        default 10000 (0 disables auto-prune)\n\
           --auto-analyze-changed-rows <N>    default 10000 (0 disables auto-analyze)\n\
           --shutdown-timeout-ms <MS>         default 30000\n\
           --deadlock-timeout-ms <MS>         default 1000\n\
           --tls-cert-file <PATH>             PEM cert chain; enables TLS (needs --tls-key-file)\n\
           --tls-key-file <PATH>              PEM private key; enables TLS (needs --tls-cert-file)\n\
           --help                             print usage and exit 0\n"
    )
}

fn next_value(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))?
        .into_string()
        .map_err(|_| format!("value for {flag} must be valid UTF-8"))
}

fn parse_port(value: &str) -> Result<u16, String> {
    let port = value
        .parse::<u16>()
        .map_err(|_| format!("invalid port: {value}"))?;
    if port == 0 {
        return Err("port must be in range 1..=65535".to_string());
    }
    Ok(port)
}

fn parse_positive_u64(value: &str, flag: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("invalid value for {flag}: {value}"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be positive"));
    }
    Ok(parsed)
}

/// Parse a `u64` allowing `0` (used by flags where 0 is a meaningful "disabled"
/// value, e.g. `--auto-vacuum-dead-rows`).
fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("invalid value for {flag}: {value}"))
}

fn parse_positive_usize(value: &str, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("invalid value for {flag}: {value}"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be positive"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{Config, ConfigAction, parse_args};

    #[test]
    fn defaults_match_server_spec() {
        let config = Config::default();

        assert_eq!(config.data_dir, std::path::PathBuf::from("./data"));
        assert_eq!(config.port, 5433);
        assert_eq!(config.buffer_pool_frames, 1024);
        assert_eq!(config.checkpoint_every_n_commits, 100);
        assert_eq!(config.checkpoint_wal_bytes, 64 * 1024 * 1024);
        assert_eq!(config.auto_vacuum_dead_rows, 10000);
        assert_eq!(config.auto_analyze_changed_rows, 10000);
        assert_eq!(config.shutdown_timeout_ms, 30000);
        assert_eq!(config.deadlock_timeout_ms, 1000);
        assert_eq!(config.tls_cert_file, None);
        assert_eq!(config.tls_key_file, None);
    }

    #[test]
    fn parses_tls_cert_and_key_flags() {
        let parsed = parse_args([
            "saguarodb",
            "--tls-cert-file",
            "/etc/saguaro/server.crt",
            "--tls-key-file",
            "/etc/saguaro/server.key",
        ])
        .unwrap();

        let ConfigAction::Run(config) = parsed else {
            panic!("expected runnable config");
        };
        assert_eq!(
            config.tls_cert_file,
            Some(PathBuf::from("/etc/saguaro/server.crt"))
        );
        assert_eq!(
            config.tls_key_file,
            Some(PathBuf::from("/etc/saguaro/server.key"))
        );
    }

    #[test]
    fn rejects_tls_cert_without_key() {
        assert!(parse_args(["saguarodb", "--tls-cert-file", "server.crt"]).is_err());
    }

    #[test]
    fn rejects_tls_key_without_cert() {
        assert!(parse_args(["saguarodb", "--tls-key-file", "server.key"]).is_err());
    }

    #[test]
    fn parses_all_config_flags() {
        let parsed = parse_args([
            "saguarodb",
            "--data-dir",
            "/tmp/saguaro",
            "--port",
            "15433",
            "--buffer-pool-frames",
            "32",
            "--checkpoint-every-n-commits",
            "5",
            "--checkpoint-wal-bytes",
            "4096",
            "--auto-vacuum-dead-rows",
            "250",
            "--shutdown-timeout-ms",
            "99",
            "--deadlock-timeout-ms",
            "250",
        ])
        .unwrap();

        let ConfigAction::Run(config) = parsed else {
            panic!("expected runnable config");
        };
        assert_eq!(config.data_dir, PathBuf::from("/tmp/saguaro"));
        assert_eq!(config.port, 15433);
        assert_eq!(config.buffer_pool_frames, 32);
        assert_eq!(config.checkpoint_every_n_commits, 5);
        assert_eq!(config.checkpoint_wal_bytes, 4096);
        assert_eq!(config.auto_vacuum_dead_rows, 250);
        assert_eq!(config.shutdown_timeout_ms, 99);
        assert_eq!(config.deadlock_timeout_ms, 250);
    }

    #[test]
    fn rejects_non_positive_deadlock_timeout() {
        assert!(parse_args(["saguarodb", "--deadlock-timeout-ms", "0"]).is_err());
    }

    #[test]
    fn auto_vacuum_dead_rows_accepts_zero_to_disable() {
        let parsed = parse_args(["saguarodb", "--auto-vacuum-dead-rows", "0"]).unwrap();
        let ConfigAction::Run(config) = parsed else {
            panic!("expected runnable config");
        };
        assert_eq!(config.auto_vacuum_dead_rows, 0);
    }

    #[test]
    fn rejects_zero_numeric_values() {
        assert!(parse_args(["saguarodb", "--port", "0"]).is_err());
        assert!(parse_args(["saguarodb", "--checkpoint-wal-bytes", "0"]).is_err());
    }

    #[test]
    fn help_is_reported_without_config() {
        assert_eq!(
            parse_args(["saguarodb", "--help"]).unwrap(),
            ConfigAction::Help
        );
    }
}
