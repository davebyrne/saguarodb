#![cfg_attr(
    not(test),
    deny(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use common::{DbError, Result};
use saguarodb_server::config::{ConfigAction, parse_args, usage};
use saguarodb_server::{app, connection, recovery, shutdown};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ExitCode {
    match async_main().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

async fn async_main() -> Result<()> {
    let program = env::args()
        .next()
        .unwrap_or_else(|| "saguarodb".to_string());
    let config = match parse_args(env::args()) {
        Ok(ConfigAction::Run(config)) => config,
        Ok(ConfigAction::Help) => {
            print!("{}", usage(&program));
            return Ok(());
        }
        Err(err) => {
            eprintln!("{err}\n{}", usage(&program));
            std::process::exit(2);
        }
    };

    let app = Arc::new(recovery::open_app(config)?);
    let bind_addr = format!("0.0.0.0:{}", app.components.config.port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|err| DbError::io(format!("failed to bind {bind_addr}: {err}")))?;

    tokio::select! {
        result = accept_loop(listener, app.clone()) => result,
        signal = shutdown::wait_for_shutdown_signal() => {
            signal?;
            shutdown::run_graceful_shutdown(app).await
        }
    }
}

async fn accept_loop(listener: TcpListener, app: Arc<app::AppState>) -> Result<()> {
    while app.components.shutdown.is_accepting() {
        let (socket, _) = listener
            .accept()
            .await
            .map_err(|err| DbError::io(format!("failed to accept connection: {err}")))?;
        let connection_app = app.clone();
        tokio::spawn(async move {
            if let Err(err) = connection::handle_connection(socket, connection_app).await {
                eprintln!("connection failed: {err}");
            }
        });
    }
    Ok(())
}
