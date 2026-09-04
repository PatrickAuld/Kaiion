use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::configure::{Client, ConfigureOptions};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
pub enum Mode {
    Batch,
    Direct,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Direct => "direct",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Args)]
pub struct Config {
    #[arg(
        long,
        global = true,
        env = "KAIION_LISTEN",
        default_value = "127.0.0.1:8787"
    )]
    pub listen: SocketAddr,

    #[arg(
        long,
        global = true,
        env = "KAIION_DATABASE_URL",
        default_value_t = default_database_url()
    )]
    pub database_url: String,

    #[arg(
        long,
        global = true,
        env = "KAIION_MODE",
        value_enum,
        default_value = "batch"
    )]
    pub mode: Mode,

    #[arg(
        long,
        global = true,
        env = "KAIION_OPENAI_BASE_URL",
        default_value = "https://api.openai.com/v1"
    )]
    pub upstream_base_url: String,

    #[arg(
        long,
        global = true,
        env = "KAIION_POLL_INTERVAL_SECONDS",
        default_value_t = 5
    )]
    pub poll_interval_seconds: u64,

    #[arg(
        long,
        global = true,
        env = "KAIION_IN_PROGRESS_INTERVAL_SECONDS",
        default_value_t = 15
    )]
    pub in_progress_interval_seconds: u64,

    #[arg(
        long,
        global = true,
        env = "KAIION_MAX_BODY_BYTES",
        default_value_t = 67_108_864
    )]
    pub max_body_bytes: usize,
}

impl Config {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds.max(1))
    }

    pub fn in_progress_interval(&self) -> Duration {
        Duration::from_secs(self.in_progress_interval_seconds.max(1))
    }

    pub fn to_args(&self) -> Vec<String> {
        vec![
            "--listen".to_string(),
            self.listen.to_string(),
            "--database-url".to_string(),
            self.database_url.clone(),
            "--mode".to_string(),
            self.mode.as_str().to_string(),
            "--upstream-base-url".to_string(),
            self.upstream_base_url.clone(),
            "--poll-interval-seconds".to_string(),
            self.poll_interval_seconds.to_string(),
            "--in-progress-interval-seconds".to_string(),
            self.in_progress_interval_seconds.to_string(),
            "--max-body-bytes".to_string(),
            self.max_body_bytes.to_string(),
        ]
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "kaiion",
    version,
    about = "A durable OpenAI Responses API proxy"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub server: Config,

    #[arg(long, global = true, hide = true)]
    pub foreground: bool,

    #[arg(long, global = true, env = "KAIION_STATE_DIR")]
    pub state_dir: Option<PathBuf>,

    #[arg(long, global = true, env = "KAIION_CONFIG_DIR")]
    pub config_dir: Option<PathBuf>,

    #[arg(long, global = true, env = "KAIION_PID_FILE")]
    pub pid_file: Option<PathBuf>,

    #[arg(long, global = true, env = "KAIION_LOG_FILE")]
    pub log_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "Start Kaiion as a background service")]
    Start,
    #[command(about = "Stop the background service")]
    Stop {
        #[arg(long, help = "Send SIGKILL immediately instead of a graceful stop")]
        force: bool,
    },
    #[command(about = "Restart the background service")]
    Restart {
        #[arg(long, help = "Send SIGKILL if graceful stop does not complete")]
        force: bool,
    },
    #[command(about = "Show process and health status")]
    Status {
        #[arg(long, help = "Print machine-readable JSON")]
        json: bool,
    },
    #[command(about = "Print recent service logs")]
    Logs {
        #[arg(long, default_value_t = 100)]
        lines: usize,
    },
    #[command(about = "Configure supported coding agents to use Kaiion")]
    Configure(ConfigureCommand),
}

#[derive(Debug, Args)]
pub struct ConfigureCommand {
    #[arg(
        long = "client",
        value_enum,
        value_delimiter = ',',
        help = "codex, opencode, pi, or claude"
    )]
    pub clients: Vec<Client>,

    #[arg(long, help = "Override the local proxy URL")]
    pub proxy_url: Option<String>,

    #[arg(
        long,
        default_value = "gpt-5.6",
        help = "Model ID to register for OpenCode and Pi"
    )]
    pub model: String,

    #[arg(
        long,
        help = "Use batch mode for clients that provide Kaiion session metadata"
    )]
    pub batch: bool,

    #[arg(long, help = "Show changes without writing files")]
    pub dry_run: bool,

    #[arg(long, help = "Home directory containing client configuration")]
    pub home: Option<PathBuf>,

    #[arg(long, help = "Override the Codex configuration directory")]
    pub codex_home: Option<PathBuf>,
}

impl Cli {
    pub fn lifecycle_paths(&self) -> LifecyclePaths {
        let state_dir = self.state_dir.clone().unwrap_or_else(default_state_dir);
        let config_dir = self.config_dir.clone().unwrap_or_else(default_config_dir);
        LifecyclePaths {
            pid_file: self
                .pid_file
                .clone()
                .unwrap_or_else(|| state_dir.join("kaiion.pid")),
            log_file: self
                .log_file
                .clone()
                .unwrap_or_else(|| state_dir.join("kaiion.log")),
            ready_file: state_dir.join("kaiion.ready"),
            config_file: config_dir.join("config.json"),
            config_dir,
            state_dir,
        }
    }

    pub fn configure_options(&self, command: &ConfigureCommand) -> ConfigureOptions {
        let home = command.home.clone().unwrap_or_else(default_home_dir);
        let proxy_url = command
            .proxy_url
            .clone()
            .unwrap_or_else(|| format!("http://{}/v1", self.server.listen));
        ConfigureOptions {
            clients: command.clients.clone(),
            home,
            codex_home: command.codex_home.clone().or_else(|| {
                if command.home.is_none() {
                    env::var_os("CODEX_HOME").map(PathBuf::from)
                } else {
                    None
                }
            }),
            proxy_url,
            model: command.model.clone(),
            direct_mode: !command.batch,
            dry_run: command.dry_run,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LifecyclePaths {
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
    pub pid_file: PathBuf,
    pub log_file: PathBuf,
    pub ready_file: PathBuf,
    pub config_file: PathBuf,
}

fn default_home_dir() -> PathBuf {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn default_database_url() -> String {
    format!(
        "sqlite://{}?mode=rwc",
        default_state_dir().join("kaiion.db").display()
    )
}

fn default_config_dir() -> PathBuf {
    env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| default_home_dir().join(".config"))
        .join("kaiion")
}

fn default_state_dir() -> PathBuf {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| default_home_dir().join(".local/state"))
        .join("kaiion")
}
