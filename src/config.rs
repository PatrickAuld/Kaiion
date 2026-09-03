use std::{net::SocketAddr, time::Duration};

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Mode {
    Batch,
    Direct,
}

#[derive(Clone, Debug, Parser)]
#[command(name = "kaiion", version, about)]
pub struct Config {
    #[arg(long, env = "KAIION_LISTEN", default_value = "127.0.0.1:8787")]
    pub listen: SocketAddr,

    #[arg(
        long,
        env = "KAIION_DATABASE_URL",
        default_value = "sqlite://kaiion.db?mode=rwc"
    )]
    pub database_url: String,

    #[arg(long, env = "KAIION_MODE", value_enum, default_value = "batch")]
    pub mode: Mode,

    #[arg(
        long,
        env = "KAIION_OPENAI_BASE_URL",
        default_value = "https://api.openai.com/v1"
    )]
    pub upstream_base_url: String,

    #[arg(
        long,
        env = "KAIION_POLL_INTERVAL_SECONDS",
        default_value_t = 5
    )]
    pub poll_interval_seconds: u64,

    #[arg(
        long,
        env = "KAIION_IN_PROGRESS_INTERVAL_SECONDS",
        default_value_t = 15
    )]
    pub in_progress_interval_seconds: u64,

    #[arg(
        long,
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
}

