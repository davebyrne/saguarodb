use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub data_dir: PathBuf,
    pub port: u16,
    pub buffer_pool_frames: usize,
    pub checkpoint_every_n_commits: u64,
    pub checkpoint_wal_bytes: u64,
    pub shutdown_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            port: 5433,
            buffer_pool_frames: 1024,
            checkpoint_every_n_commits: 100,
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            shutdown_timeout_ms: 30000,
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
            "--shutdown-timeout-ms" => {
                let value = next_value(&mut args, "--shutdown-timeout-ms")?;
                config.shutdown_timeout_ms = parse_positive_u64(&value, "--shutdown-timeout-ms")?;
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

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
           --shutdown-timeout-ms <MS>         default 30000\n\
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
        assert_eq!(config.shutdown_timeout_ms, 30000);
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
            "--shutdown-timeout-ms",
            "99",
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
        assert_eq!(config.shutdown_timeout_ms, 99);
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
