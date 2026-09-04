use std::{
    env,
    fs::{self, OpenOptions},
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::config::{Config, LifecyclePaths};

pub async fn start(config: &Config, paths: &LifecyclePaths) -> Result<(), String> {
    let working_directory = env::current_dir().map_err(io_error("locate working directory"))?;
    start_at(config, paths, &working_directory).await
}

pub async fn restart(config: &Config, paths: &LifecyclePaths, force: bool) -> Result<(), String> {
    let saved = load_state(paths)?;
    stop(paths, force).await?;
    if let Some(saved) = saved {
        start_at(&saved.config, paths, &saved.working_directory).await
    } else {
        start(config, paths).await
    }
}

async fn start_at(
    config: &Config,
    paths: &LifecyclePaths,
    working_directory: &Path,
) -> Result<(), String> {
    crate::atomic_file::ensure_private_directory(&paths.state_dir)
        .map_err(io_error("create state directory"))?;
    crate::atomic_file::ensure_private_directory(&paths.config_dir)
        .map_err(io_error("create config directory"))?;
    ensure_parent(&paths.pid_file)?;
    ensure_parent(&paths.log_file)?;
    ensure_parent(&paths.config_file)?;
    if let Some(pid) = read_pid(&paths.pid_file)? {
        if process_alive(pid) && !is_owned_process(pid) {
            return Err(format!(
                "PID file {} belongs to another process",
                paths.pid_file.display()
            ));
        }
        if process_alive(pid) {
            return Err(format!("Kaiion is already running with PID {pid}"));
        }
        remove_pid(&paths.pid_file)?;
    }
    remove_if_exists(&paths.ready_file)?;
    write_state(
        paths,
        &PersistedState {
            config: config.clone(),
            working_directory: working_directory.to_path_buf(),
        },
    )?;

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)
        .map_err(io_error("open log file"))?;
    let stderr = log.try_clone().map_err(io_error("clone log file"))?;
    let executable = env::current_exe().map_err(io_error("locate kaiion executable"))?;
    let child = Command::new(executable)
        .arg("--foreground")
        .args(config.to_args())
        .env_remove("KAIION_ROUTING_POLICY")
        .env_remove("KAIION_RESUME_FROM_ENV")
        .env("KAIION_READY_FILE", &paths.ready_file)
        .current_dir(working_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(io_error("start Kaiion"))?;
    let pid = child.id();
    if let Err(error) = write_pid(&paths.pid_file, pid) {
        let _ = terminate_pid(pid, true);
        let _ = child.wait_with_output();
        return Err(error);
    }

    let mut child = child;
    if wait_for_ready(
        &mut child,
        &paths.ready_file,
        config.listen,
        Duration::from_secs(10),
    )
    .await
    {
        println!("Kaiion started (PID {pid}) on http://{}", config.listen);
        Ok(())
    } else {
        let _ = terminate_pid(pid, true);
        let _ = child.wait();
        remove_pid(&paths.pid_file)?;
        remove_if_exists(&paths.ready_file)?;
        Err(format!(
            "Kaiion did not become healthy; inspect {}",
            paths.log_file.display()
        ))
    }
}

pub async fn stop(paths: &LifecyclePaths, force: bool) -> Result<(), String> {
    let Some(pid) = read_pid(&paths.pid_file)? else {
        println!("Kaiion is not running");
        return Ok(());
    };
    if process_alive(pid) && !is_owned_process(pid) {
        return Err(format!(
            "PID file {} belongs to another process",
            paths.pid_file.display()
        ));
    }
    if !process_alive(pid) {
        remove_pid(&paths.pid_file)?;
        remove_if_exists(&paths.ready_file)?;
        println!("Kaiion is not running (removed stale PID file)");
        return Ok(());
    }

    terminate_pid(pid, force).map_err(|error| format!("could not stop PID {pid}: {error}"))?;
    let deadline = Instant::now() + Duration::from_secs(if force { 2 } else { 15 });
    while process_alive(pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if process_alive(pid) {
        if force {
            return Err(format!("Kaiion PID {pid} did not exit"));
        }
        terminate_pid(pid, true)
            .map_err(|error| format!("could not force stop PID {pid}: {error}"))?;
        let force_deadline = Instant::now() + Duration::from_secs(2);
        while process_alive(pid) && Instant::now() < force_deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    if process_alive(pid) {
        return Err(format!("Kaiion PID {pid} did not exit"));
    }
    remove_pid(&paths.pid_file)?;
    remove_if_exists(&paths.ready_file)?;
    println!("Kaiion stopped");
    Ok(())
}

pub async fn status(config: &Config, paths: &LifecyclePaths, json: bool) -> Result<(), String> {
    let saved = load_state(paths)?;
    let config = saved.as_ref().map_or(config, |saved| &saved.config);
    let pid = read_pid(&paths.pid_file)?;
    let running = pid.is_some_and(|pid| process_alive(pid) && is_owned_process(pid));
    let healthy = if running {
        health(config.listen).await
    } else {
        false
    };
    if json {
        println!(
            "{}",
            serde_json::json!({
                "running": running,
                "healthy": healthy,
                "pid": pid,
                "listen": config.listen.to_string(),
                "pid_file": paths.pid_file,
                "log_file": paths.log_file,
            })
        );
    } else if healthy {
        println!(
            "Kaiion is running (PID {}) on http://{}",
            pid.unwrap(),
            config.listen
        );
    } else if running {
        println!("Kaiion process {} is running but unhealthy", pid.unwrap());
    } else {
        println!("Kaiion is stopped");
    }
    Ok(())
}

pub fn logs(paths: &LifecyclePaths, lines: usize) -> Result<(), String> {
    let content = match fs::read_to_string(&paths.log_file) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            println!("no Kaiion log file at {}", paths.log_file.display());
            return Ok(());
        }
        Err(error) => return Err(io_error("read log file")(error)),
    };
    let all: Vec<&str> = content.lines().collect();
    let start = all.len().saturating_sub(lines);
    for line in &all[start..] {
        println!("{line}");
    }
    Ok(())
}

async fn wait_for_ready(
    child: &mut std::process::Child,
    ready_file: &Path,
    listen: SocketAddr,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return false;
        }
        if ready_file.exists() && health(listen).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn health(listen: SocketAddr) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
    else {
        return false;
    };
    client
        .get(format!("http://{listen}/healthz"))
        .send()
        .await
        .is_ok_and(|response| response.status() == StatusCode::OK)
}

fn write_pid(path: &Path, pid: u32) -> Result<(), String> {
    crate::atomic_file::write(path, format!("{pid}\n").as_bytes())
        .map_err(io_error("write PID file"))
}

fn read_pid(path: &Path) -> Result<Option<u32>, String> {
    match fs::read_to_string(path) {
        Ok(value) => {
            let pid = value
                .trim()
                .parse::<u32>()
                .ok()
                .filter(|pid| valid_pid(*pid))
                .ok_or_else(|| format!("invalid PID file {}", path.display()))?;
            Ok(Some(pid))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read PID file")(error)),
    }
}

fn valid_pid(pid: u32) -> bool {
    #[cfg(unix)]
    {
        pid > 0 && pid <= i32::MAX as u32
    }
    #[cfg(windows)]
    {
        pid > 0
    }
}

fn remove_pid(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove PID file")(error)),
    }
}

fn remove_if_exists(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove lifecycle file")(error)),
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct PersistedState {
    config: Config,
    working_directory: PathBuf,
}

fn load_state(paths: &LifecyclePaths) -> Result<Option<PersistedState>, String> {
    match fs::read_to_string(&paths.config_file) {
        Ok(content) => serde_json::from_str(&content)
            .map(Some)
            .map_err(|error| format!("could not parse {}: {error}", paths.config_file.display())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read lifecycle configuration")(error)),
    }
}

fn write_state(paths: &LifecyclePaths, state: &PersistedState) -> Result<(), String> {
    let content = serde_json::to_string_pretty(state)
        .map_err(|error| format!("could not serialize lifecycle configuration: {error}"))?;
    crate::atomic_file::write(&paths.config_file, format!("{content}\n").as_bytes())
        .map_err(io_error("write lifecycle file"))
}

fn process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(pid as libc::pid_t, 0) == 0
                || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        }
    }
    #[cfg(windows)]
    {
        Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).contains(&pid.to_string()))
    }
}

fn is_owned_process(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(executable) = env::current_exe() else {
            return false;
        };
        fs::read_link(format!("/proc/{pid}/exe"))
            .map(|running| running == executable)
            .unwrap_or(true)
    }
    #[cfg(windows)]
    {
        let _ = pid;
        true
    }
}

fn terminate_pid(pid: u32, force: bool) -> Result<(), io::Error> {
    #[cfg(unix)]
    {
        let signal = if force { libc::SIGKILL } else { libc::SIGTERM };
        let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
        if result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other("taskkill failed"))
        }
    }
}

fn io_error(action: &'static str) -> impl FnOnce(io::Error) -> String {
    move |error| format!("could not {action}: {error}")
}

fn ensure_parent(path: &Path) -> Result<(), String> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(io_error("create lifecycle directory"))?;
    }
    Ok(())
}
