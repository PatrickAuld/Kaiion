use std::{net::SocketAddr, path::Path, process::Stdio, time::Duration};

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
    let address = unused_address();
    let database_url = format!("sqlite://{}?mode=rwc", database.display());
    let child = Command::new(env!("CARGO_BIN_EXE_kaiion"))
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
