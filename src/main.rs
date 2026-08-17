#![cfg_attr(
    not(test),
    deny(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)
)]

mod app;
mod config;
mod mapping;
mod query;

use config::Config;

#[tokio::main]
async fn main() -> capability::ExitCode {
    match run().await {
        Ok(()) => capability::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            capability::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let config = Config::from_env()?;
    let port = config.http_port;
    let state = app::build_state(config)?;
    let router = app::router(state);

    eprintln!("espg listening on {port}");
    capability::serve(router, port).await
}

// strictrs: capability
mod capability {
    pub use std::process::ExitCode;

    use axum::Router;
    use tokio::net::TcpListener;

    pub async fn serve(router: Router, port: u16) -> Result<(), String> {
        let address = format!("0.0.0.0:{port}");
        let listener = TcpListener::bind(&address)
            .await
            .map_err(|error| format!("failed to bind {address}: {error}"))?;
        axum::serve(listener, router)
            .await
            .map_err(|error| format!("server error: {error}"))
    }
}
