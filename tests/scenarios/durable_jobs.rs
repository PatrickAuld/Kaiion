use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use kaiion::{
    db::Database,
    request::{NormalizedRequest, UpstreamAuth},
};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::support::*;

fn request() -> Value {
    json!({"model": "gpt-test", "input": "Investigate the next step", "store": false})
}

async fn terminal(client: &reqwest::Client, url: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result: Value = client
                .get(url)
                .bearer_auth("test-key")
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            if result["terminal"] == true {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn detached_job_survives_restart_and_resumes_without_original_request() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("jobs.db");
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/kaiion/jobs", kaiion.address))
        .bearer_auth("test-key")
        .header("idempotency-key", "workflow-a/step-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let location = response.headers()["location"].to_str().unwrap().to_string();
    let job: Value = response.json().await.unwrap();
    wait_for_batch(&provider).await;
    kaiion.stop().await;
    provider.complete_all().await;
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let url = format!("http://{}{location}", kaiion.address);
    for bad in [
        client.get(&url).bearer_auth("other-key"),
        client
            .get(&url)
            .bearer_auth("test-key")
            .header("openai-project", "another-project"),
    ] {
        assert_eq!(bad.send().await.unwrap().status(), StatusCode::NOT_FOUND);
    }
    assert_eq!(
        client
            .post(format!("{url}/resume"))
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    let done = terminal(&client, &url).await;
    assert_eq!(done["status"], "completed");
    assert_eq!(
        done["response"]["id"],
        format!("resp_kaiion_{}", job["id"].as_str().unwrap())
    );
    assert_eq!(provider.batch_creations(), 1);
    let replay: Value = client
        .post(format!("http://{}/v1/responses", kaiion.address))
        .bearer_auth("test-key")
        .header("idempotency-key", "workflow-a/step-1")
        .json(&request())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay, done["response"]);
    let mut changed = request();
    changed["input"] = json!("different");
    let conflict = client
        .post(format!("http://{}/v1/kaiion/jobs", kaiion.address))
        .bearer_auth("test-key")
        .header("idempotency-key", "workflow-a/step-1")
        .json(&changed)
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(provider.batch_creations(), 1);
    let bytes = std::fs::read(&database).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("Bearer test-key"));
    kaiion.stop().await;
}

#[tokio::test]
async fn queued_request_payload_is_durable_before_any_provider_call() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("queued.db");
    let url = format!("http://{}/v1", fake.address);
    let db = Database::connect(&format!("sqlite://{}?mode=rwc", database.display()))
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("idempotency-key", "queued-step".parse().unwrap());
    let auth = UpstreamAuth {
        authorization: "Bearer test-key".into(),
        organization: None,
        project: None,
    };
    let normalized = NormalizedRequest::from_headers(&request(), &url, &headers).unwrap();
    let job = db
        .enqueue(&auth.fingerprint(), &url, &normalized)
        .await
        .unwrap();
    drop(db);
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/kaiion/jobs/{}", kaiion.address, job.id);
    client
        .post(format!("{url}/resume"))
        .bearer_auth("test-key")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    assert_eq!(terminal(&client, &url).await["status"], "completed");
    assert_eq!(provider.batch_creations(), 1);
    kaiion.stop().await;
}

#[tokio::test]
async fn generic_session_identity_isolated_and_concurrent_idempotency_is_atomic() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("jobs.db"), fake.address).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/kaiion/jobs", kaiion.address);
    let requests = (0..12).map(|_| {
        client
            .post(&url)
            .bearer_auth("test-key")
            .header("idempotency-key", "same-step")
            .json(&request())
            .send()
    });
    let mut ids = std::collections::HashSet::new();
    for response in futures_util::future::join_all(requests).await {
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        ids.insert(
            response.json::<Value>().await.unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    assert_eq!(ids.len(), 1);
    for session in ["workflow-one", "workflow-two"] {
        let result: Value = client
            .post(&url)
            .bearer_auth("test-key")
            .header("x-kaiion-session-id", session)
            .json(&request())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.insert(result["id"].as_str().unwrap().to_string());
    }
    assert_eq!(ids.len(), 3);
    wait_for_batch_count(&provider, 3).await;
    assert_eq!(provider.batch_creations(), 3);
    kaiion.stop().await;
}

#[tokio::test]
async fn environment_credentials_resume_only_owned_jobs_after_restart() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("startup.db");
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let client = reqwest::Client::new();
    let result: Value = client
        .post(format!("http://{}/v1/kaiion/jobs", kaiion.address))
        .bearer_auth("test-key")
        .header("idempotency-key", "startup-step")
        .json(&request())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    wait_for_batch(&provider).await;
    kaiion.stop().await;
    provider.complete_all().await;
    let kaiion = crate::support::process::start_kaiion_with_env(
        "batch",
        &database,
        fake.address,
        &["--resume-from-env"],
        &[("OPENAI_API_KEY", "test-key")],
    )
    .await;
    let url = format!(
        "http://{}/v1/kaiion/jobs/{}",
        kaiion.address,
        result["id"].as_str().unwrap()
    );
    assert_eq!(terminal(&client, &url).await["status"], "completed");
    let list_url = format!("http://{}/v1/kaiion/jobs", kaiion.address);
    let own: Value = client
        .get(&list_url)
        .bearer_auth("test-key")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(own["data"].as_array().unwrap().len(), 1);
    let other: Value = client
        .get(&list_url)
        .bearer_auth("other-key")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(other["data"].as_array().unwrap().is_empty());
    assert_eq!(provider.batch_creations(), 1);
    kaiion.stop().await;
    let second_provider = FakeProvider::default();
    let second_fake = spawn_fake_provider(second_provider.clone()).await;
    let kaiion = start_kaiion("batch", &database, second_fake.address).await;
    let url = format!(
        "http://{}/v1/kaiion/jobs/{}",
        kaiion.address,
        result["id"].as_str().unwrap()
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn non_streaming_client_waits_for_batch_and_can_replay_as_sse() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("json.db"), fake.address).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/responses", kaiion.address);
    let mut body = codex_request("json");
    body["stream"] = json!(false);
    body.as_object_mut().unwrap().remove("store");
    let sending = client.post(&url).bearer_auth("test-key").json(&body).send();
    let complete = async {
        wait_for_batch(&provider).await;
        provider.complete_all().await;
    };
    let (response, _) = tokio::join!(sending, complete);
    let response = response.unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-kaiion-mode"], "batch");
    let result: Value = response.json().await.unwrap();
    assert_eq!(result["status"], "completed");
    assert_eq!(
        provider.inner.uploaded_batch_lines.lock().await[0]["body"]["store"],
        false
    );
    body["stream"] = json!(true);
    let mut replay = FakeCodex::new(kaiion.address).send(&body).await;
    expect_batch_lifecycle_start(&mut replay).await;
    let events = replay.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.completed");
    assert_eq!(provider.batch_creations(), 1);
    kaiion.stop().await;
}

#[tokio::test]
async fn cli_submits_and_waits_for_a_detached_job_with_environment_auth() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("cli.db"), fake.address).await;
    let request_path = directory.path().join("request.json");
    std::fs::write(&request_path, request().to_string()).unwrap();
    let origin = format!("http://{}", kaiion.address);
    let submitted = tokio::process::Command::new(env!("CARGO_BIN_EXE_kaiiron"))
        .args([
            "jobs",
            "--proxy-url",
            &origin,
            "submit",
            "--request",
            request_path.to_str().unwrap(),
            "--idempotency-key",
            "cli-step",
        ])
        .env("OPENAI_API_KEY", "test-key")
        .env_remove("OPENAI_ORG_ID")
        .env_remove("OPENAI_PROJECT_ID")
        .output()
        .await
        .unwrap();
    assert!(
        submitted.status.success(),
        "{}",
        String::from_utf8_lossy(&submitted.stderr)
    );
    let job: Value = serde_json::from_slice(&submitted.stdout).unwrap();
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    let waited = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_kaiiron"))
            .args([
                "jobs",
                "--proxy-url",
                &origin,
                "wait",
                job["id"].as_str().unwrap(),
            ])
            .env("OPENAI_API_KEY", "test-key")
            .env_remove("OPENAI_ORG_ID")
            .env_remove("OPENAI_PROJECT_ID")
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&waited.stdout).unwrap()["status"],
        "completed"
    );
    assert_eq!(provider.batch_creations(), 1);
    kaiion.stop().await;
}
