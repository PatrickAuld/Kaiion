use std::{env, fs};

use clap::Parser;
use kaiion::{Cli, Command, Config, build_router, configure, lifecycle};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.foreground || cli.command.is_none() {
        let paths = cli.lifecycle_paths();
        kaiion::atomic_file::ensure_private_directory(&paths.state_dir)?;
        kaiion::atomic_file::ensure_private_directory(&paths.config_dir)?;
        return run_server(cli.server).await;
    }

    let paths = cli.lifecycle_paths();
    let configure_options = match cli.command.as_ref() {
        Some(Command::Configure(command)) => Some(cli.configure_options(command)),
        _ => None,
    };
    match cli.command.unwrap() {
        Command::Start => lifecycle::start(&cli.server, &paths).await?,
        Command::Stop { force } => lifecycle::stop(&paths, force).await?,
        Command::Restart { force } => lifecycle::restart(&cli.server, &paths, force).await?,
        Command::Status { json } => lifecycle::status(&cli.server, &paths, json).await?,
        Command::Logs { lines } => lifecycle::logs(&paths, lines)?,
        Command::Configure(_) => configure::configure(&configure_options.unwrap())?,
        Command::Jobs(command) => kaiion::job_cli::run(command, cli.server.listen).await?,
    }
    Ok(())
}

async fn run_server(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("kaiion=info")),
        )
        .init();

    let listen = config.listen;
    let app = build_router(config).await?;
    let listener = TcpListener::bind(listen).await?;
    if let Some(ready_file) = env::var_os("KAIION_READY_FILE") {
        fs::write(ready_file, format!("{}\n", std::process::id()))?;
    }

    info!(%listen, "Kaiion listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
