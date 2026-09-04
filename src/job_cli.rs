use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Args, Subcommand};
use reqwest::{Client, Method};
use serde_json::Value;

#[derive(Debug, Args)]
pub struct JobsCommand {
    #[arg(long, help = "Proxy origin, for example http://127.0.0.1:8787")]
    pub proxy_url: Option<String>,
    #[command(subcommand)]
    pub action: JobAction,
}

#[derive(Debug, Subcommand)]
pub enum JobAction {
    List {
        #[arg(long)]
        after: Option<String>,
    },
    Submit {
        #[arg(long)]
        request: PathBuf,
        #[arg(long)]
        idempotency_key: String,
    },
    Show {
        id: String,
    },
    Resume {
        id: String,
    },
    #[command(about = "Resume polling and wait for a terminal result; safe to interrupt")]
    Wait {
        id: String,
    },
    #[command(about = "Explain auto routing without submitting inference")]
    Route {
        #[arg(long)]
        request: PathBuf,
    },
}

pub async fn run(
    command: JobsCommand,
    listen: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let origin = command
        .proxy_url
        .unwrap_or_else(|| format!("http://{listen}"));
    let mut base = reqwest::Url::parse(&origin)?;
    if !matches!(base.scheme(), "http" | "https")
        || base.query().is_some()
        || base.fragment().is_some()
        || !base.username().is_empty()
        || base.password().is_some()
    {
        return Err(
            "proxy URL must be an HTTP(S) origin without credentials, query, or fragment".into(),
        );
    }
    base.set_path("/v1/kaiion/");
    let mut headers = reqwest::header::HeaderMap::new();
    let mut authorization = reqwest::header::HeaderValue::from_str(&format!(
        "Bearer {}",
        std::env::var("OPENAI_API_KEY")?
    ))?;
    authorization.set_sensitive(true);
    headers.insert(reqwest::header::AUTHORIZATION, authorization);
    for (variable, header) in [
        ("OPENAI_ORG_ID", "openai-organization"),
        ("OPENAI_PROJECT_ID", "openai-project"),
    ] {
        if let Ok(value) = std::env::var(variable) {
            headers.insert(header, reqwest::header::HeaderValue::from_str(&value)?);
        }
    }
    let client = Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(120))
        .build()?;
    let (method, path, body, key, wait) = match command.action {
        JobAction::List { after } => {
            let mut path = "jobs".to_string();
            if let Some(after) = after {
                job_path(&after)?;
                path.push_str(&format!("?after={after}"));
            }
            (Method::GET, path, None, None, false)
        }
        JobAction::Submit {
            request,
            idempotency_key,
        } => (
            Method::POST,
            "jobs".to_string(),
            Some(read_request(request)?),
            Some(idempotency_key),
            false,
        ),
        JobAction::Route { request } => (
            Method::POST,
            "route".to_string(),
            Some(read_request(request)?),
            Some("route-preview".into()),
            false,
        ),
        JobAction::Show { id } => (Method::GET, job_path(&id)?, None, None, false),
        JobAction::Resume { id } => (
            Method::POST,
            format!("{}/resume", job_path(&id)?),
            None,
            None,
            false,
        ),
        JobAction::Wait { id } => (
            Method::POST,
            format!("{}/resume", job_path(&id)?),
            None,
            None,
            true,
        ),
    };
    let mut request = client.request(method, base.join(&path)?);
    if let Some(body) = body {
        request = request.json(&body);
    }
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    if path == "route" {
        request = request.header("x-kaiion-mode", "auto");
    }
    let mut result = decode(request.send().await?).await?;
    while wait && result.get("terminal").and_then(Value::as_bool) == Some(false) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        result = decode(
            client
                .get(base.join(path.trim_end_matches("/resume"))?)
                .send()
                .await?,
        )
        .await?;
    }
    println!("{}", serde_json::to_string_pretty(&result)?);
    if wait && result.get("status").and_then(Value::as_str) != Some("completed") {
        return Err("job reached a non-successful terminal state".into());
    }
    Ok(())
}

fn job_path(id: &str) -> Result<String, Box<dyn std::error::Error>> {
    if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("job ID must be hexadecimal".into());
    }
    Ok(format!("jobs/{id}"))
}

fn read_request(path: PathBuf) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

async fn decode(response: reqwest::Response) -> Result<Value, Box<dyn std::error::Error>> {
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("proxy returned {status}: {body}").into());
    }
    Ok(serde_json::from_str(&body)?)
}
