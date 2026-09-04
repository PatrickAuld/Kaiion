use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use tokio::process::{Child, Command};

pub struct KaiionProcess {
    child: Child,
    pub address: SocketAddr,
}

impl KaiionProcess {
    pub async fn stop(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

impl Drop for KaiionProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

pub async fn start_kaiion(mode: &str, database: &Path, upstream: SocketAddr) -> KaiionProcess {
    start_kaiion_with_args(mode, database, upstream, &[]).await
}

pub async fn start_kaiion_with_args(
    mode: &str,
    database: &Path,
    upstream: SocketAddr,
    args: &[&str],
) -> KaiionProcess {
    start_kaiion_with_env(mode, database, upstream, args, &[]).await
}

pub async fn start_kaiion_with_env(
    mode: &str,
    database: &Path,
    upstream: SocketAddr,
    args: &[&str],
    env: &[(&str, &str)],
) -> KaiionProcess {
    let address = unused_address();
    let database_url = format!("sqlite://{}?mode=rwc", database.display());
    let executable = env::var_os("KAIION_TEST_BINARY")
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_BIN_EXE_kaiion").map(PathBuf::from))
        .expect("build kaiion and set KAIION_TEST_BINARY to the executable path");
    assert!(
        executable.is_file(),
        "Kaiion test binary does not exist at {}",
        executable.display()
    );
    let child = Command::new(executable)
        .arg("--listen")
        .arg(address.to_string())
        .arg("--database-url")
        .arg(database_url)
        .arg("--upstream-base-url")
        .arg(format!("http://{upstream}/v1"))
        .arg("--mode")
        .arg(mode)
        .arg("--poll-interval-seconds")
        .arg("1")
        .arg("--in-progress-interval-seconds")
        .arg("1")
        .args(args)
        .env_remove("KAIION_ROUTING_POLICY")
        .env_remove("KAIION_RESUME_FROM_ENV")
        .envs(env.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let process = KaiionProcess { child, address };
    wait_for_health(address).await;
    process
}

fn unused_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn wait_for_health(address: SocketAddr) {
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if client
            .get(format!("http://{address}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Kaiion did not become healthy at {address}");
}
