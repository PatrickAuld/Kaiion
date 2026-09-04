use std::{
    env,
    ffi::{OsStr, OsString},
    path::Path,
    process::Stdio,
    time::Duration,
};

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

use crate::support::{FakeProvider, kaiion_binary, spawn_fake_provider, start_kaiion};

const MODEL: &str = "gpt-test";
const EXPECTED_OUTPUT: &str = "batch response";

#[derive(Clone, Copy)]
enum Client {
    Codex,
    Opencode,
    Pi,
}

impl Client {
    fn name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Opencode => "opencode",
            Self::Pi => "pi",
        }
    }

    fn binary_env(self) -> &'static str {
        match self {
            Self::Codex => "KAIION_TEST_CODEX_BINARY",
            Self::Opencode => "KAIION_TEST_OPENCODE_BINARY",
            Self::Pi => "KAIION_TEST_PI_BINARY",
        }
    }

    fn binary(self) -> OsString {
        env::var_os(self.binary_env()).unwrap_or_else(|| self.name().into())
    }
}

#[tokio::test]
async fn codex_cli_waits_for_and_consumes_a_batch_response() {
    run_client(Client::Codex).await;
}

#[tokio::test]
async fn opencode_cli_waits_for_and_consumes_a_batch_response() {
    run_client(Client::Opencode).await;
}

#[tokio::test]
async fn pi_cli_waits_for_and_consumes_a_batch_response() {
    run_client(Client::Pi).await;
}

async fn run_client(client: Client) {
    if !real_cli_tests_enabled() {
        eprintln!(
            "skipping {} CLI compatibility test; set KAIION_TEST_REAL_CLIS=1 to enable",
            client.name()
        );
        return;
    }

    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join(format!("{}.db", client.name()));
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    configure_client(client, directory.path(), kaiion.address).await;

    let mut child = spawn_client(client, directory.path());
    if matches!(client, Client::Codex) {
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(b"Reply with the supplied result and do not use tools.\n")
            .await
            .unwrap();
        stdin.shutdown().await.unwrap();
    }
    wait_for_batch_while_running(&provider, &mut child, client).await;
    assert_eq!(provider.batch_statuses().await, ["in_progress"]);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "{} exited before the mock provider completed its batch",
        client.name()
    );

    provider.complete_all().await;
    let (status, stdout_bytes, stderr_bytes) = wait_for_client_exit(child, client).await;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    let stderr = String::from_utf8_lossy(&stderr_bytes);
    assert!(
        status.success(),
        "{} failed with {}\nstdout:\n{}\nstderr:\n{}",
        client.name(),
        status,
        stdout,
        stderr
    );
    assert!(
        stdout.contains(EXPECTED_OUTPUT) || stderr.contains(EXPECTED_OUTPUT),
        "{} did not render the batch result\nstdout:\n{}\nstderr:\n{}",
        client.name(),
        stdout,
        stderr
    );
    assert_eq!(provider.batch_creations(), 1);
    let requests = provider.inner.uploaded_batch_lines.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["body"]["model"], MODEL);
    assert_eq!(requests[0]["body"]["store"], false);
    assert_eq!(requests[0]["body"]["stream"], false);
    assert!(requests[0]["body"].to_string().contains("supplied result"));
    drop(requests);
    for call in provider.calls().await {
        assert_eq!(call.authorization, "Bearer black-box-key", "{call:?}");
    }
    kaiion.stop().await;
}

async fn wait_for_batch_while_running(provider: &FakeProvider, child: &mut Child, client: Client) {
    for _ in 0..400 {
        if provider.batch_creations() >= 1 {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            let (stdout, stderr) = read_child_output(child).await;
            panic!(
                "{} exited with {} before creating a batch\nstdout:\n{}\nstderr:\n{}",
                client.name(),
                status,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let status = kill_client(child).await;
    let (stdout, stderr) = read_child_output(child).await;
    panic!(
        "{} did not create a batch; killed with {}\nstdout:\n{}\nstderr:\n{}",
        client.name(),
        status,
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
}

async fn wait_for_client_exit(
    mut child: Child,
    client: Client,
) -> (std::process::ExitStatus, Vec<u8>, Vec<u8>) {
    for _ in 0..1200 {
        if let Some(status) = child.try_wait().unwrap() {
            let (stdout, stderr) = read_child_output(&mut child).await;
            return (status, stdout, stderr);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let status = kill_client(&mut child).await;
    let (stdout, stderr) = read_child_output(&mut child).await;
    panic!(
        "{} did not exit after batch completion; killed with {}\nstdout:\n{}\nstderr:\n{}",
        client.name(),
        status,
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
}

async fn read_child_output(child: &mut Child) -> (Vec<u8>, Vec<u8>) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout).await.unwrap();
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr).await.unwrap();
    }
    (stdout, stderr)
}

async fn kill_client(child: &mut Child) -> std::process::ExitStatus {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    child.wait().await.unwrap()
}

fn real_cli_tests_enabled() -> bool {
    env::var("KAIION_TEST_REAL_CLIS")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

async fn configure_client(client: Client, home: &Path, address: std::net::SocketAddr) {
    let mut command = Command::new(kaiion_binary());
    command
        .arg("configure")
        .arg("--client")
        .arg(client.name())
        .arg("--proxy-url")
        .arg(format!("http://{address}/v1"))
        .arg("--model")
        .arg(MODEL)
        .arg("--client-mode")
        .arg("batch")
        .arg("--session-id")
        .arg(format!("black-box-{}", client.name()))
        .arg("--home")
        .arg(home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if matches!(client, Client::Codex) {
        command.arg("--codex-home").arg(home.join(".codex"));
    }
    let output = command.output().await.unwrap();
    assert!(
        output.status.success(),
        "configuration for {} failed with {}\nstdout:\n{}\nstderr:\n{}",
        client.name(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn spawn_client(client: Client, home: &Path) -> Child {
    let workspace = home.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let mut command = Command::new(client.binary());
    command
        .current_dir(&workspace)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local/state"))
        .env("CODEX_HOME", home.join(".codex"))
        .env("PI_CODING_AGENT_DIR", home.join(".pi/agent"))
        .env("PI_OFFLINE", "1")
        .env("OPENCODE_DISABLE_AUTOUPDATE", "true")
        .env("OPENCODE_DISABLE_MODELS_FETCH", "true")
        .env("OPENCODE_DISABLE_DEFAULT_PLUGINS", "true")
        .env("OPENCODE_DISABLE_LSP_DOWNLOAD", "true")
        .env("OPENCODE_DISABLE_CLAUDE_CODE", "true")
        .env("OPENCODE_DISABLE_SHARE", "true")
        .env("OPENCODE_DISABLE_EXTERNAL_SKILLS", "true")
        .env("OPENCODE_DISABLE_EMBEDDED_WEB_UI", "true")
        .env("OPENCODE_DISABLE_PRUNE", "true")
        .env("OPENCODE_FAST_BOOT", "true")
        .env("DO_NOT_TRACK", "1")
        .env("OPENAI_API_KEY", "black-box-key")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env_remove("OPENAI_ORG_ID")
        .env_remove("OPENAI_PROJECT_ID")
        .env_remove("OPENCODE_CONFIG")
        .env_remove("OPENCODE_CONFIG_CONTENT")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("HTTP_PROXY")
        .env_remove("HTTPS_PROXY")
        .env_remove("ALL_PROXY")
        .env_remove("http_proxy")
        .env_remove("https_proxy")
        .env_remove("all_proxy")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    match client {
        Client::Codex => {
            command.stdin(Stdio::piped());
            command.args([
                OsStr::new("exec"),
                OsStr::new("--ephemeral"),
                OsStr::new("--skip-git-repo-check"),
                OsStr::new("--sandbox"),
                OsStr::new("read-only"),
                OsStr::new("--color"),
                OsStr::new("never"),
                OsStr::new("--model"),
                OsStr::new(MODEL),
                OsStr::new("-"),
            ]);
        }
        Client::Opencode => {
            command.stdin(Stdio::null());
            command.args([
                OsStr::new("run"),
                OsStr::new("--pure"),
                OsStr::new("--print-logs"),
                OsStr::new("--log-level"),
                OsStr::new("DEBUG"),
                OsStr::new("--title"),
                OsStr::new("kaiion-black-box"),
                OsStr::new("--model"),
                OsStr::new("kaiion/gpt-test"),
                OsStr::new("--format"),
                OsStr::new("json"),
                OsStr::new("Reply with the supplied result and do not use tools."),
            ]);
        }
        Client::Pi => {
            command.stdin(Stdio::null());
            command.args([
                OsStr::new("--provider"),
                OsStr::new("kaiion"),
                OsStr::new("--model"),
                OsStr::new(MODEL),
                OsStr::new("--print"),
                OsStr::new("--no-session"),
                OsStr::new("--no-tools"),
                OsStr::new("--no-extensions"),
                OsStr::new("--no-skills"),
                OsStr::new("--no-prompt-templates"),
                OsStr::new("--no-context-files"),
                OsStr::new("--offline"),
                OsStr::new("Reply with the supplied result and do not use tools."),
            ]);
        }
    }
    command.spawn().unwrap_or_else(|error| {
        panic!(
            "could not launch {} from {}: {error}",
            client.name(),
            client.binary_env()
        )
    })
}
