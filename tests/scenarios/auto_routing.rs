use serde_json::{Value, json};
use tempfile::TempDir;

use crate::support::process::start_kaiion_with_args;
use crate::support::*;

fn policy() -> Value {
    json!({"max_direct_cost_usd": 0.001, "max_direct_premium_usd": 0.0005, "models": {"gpt-test": {
        "input_usd_per_million": 1, "output_usd_per_million": 4,
        "batch_input_usd_per_million": 0.5, "batch_output_usd_per_million": 2
    }}})
}

#[tokio::test]
async fn auto_routes_cheap_calls_direct_and_reasoning_to_batch_with_explanations() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let policy_path = directory.path().join("routing.json");
    std::fs::write(&policy_path, policy().to_string()).unwrap();
    let kaiion = start_kaiion_with_args(
        "auto",
        &directory.path().join("jobs.db"),
        fake.address,
        &["--routing-policy", policy_path.to_str().unwrap()],
    )
    .await;
    let client = reqwest::Client::new();
    let mut body = codex_request("auto");
    body["max_output_tokens"] = json!(64);
    let explained: Value = client
        .post(format!("http://{}/v1/kaiion/route", kaiion.address))
        .bearer_auth("test-key")
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(explained["reason"], "within_direct_allowance");
    assert!(explained["estimated_direct_cost_usd"].as_f64().unwrap() < 0.001);
    assert_eq!(provider.calls().await.len(), 0);
    let codex = FakeCodex::new(kaiion.address);
    let response = codex.send(&body).await;
    assert_eq!(response.headers["x-kaiion-mode"], "direct");
    assert_eq!(
        response.headers["x-kaiion-route-reason"],
        "within_direct_allowance"
    );
    assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    body["reasoning"] = json!({"effort": "high"});
    let mut response = codex.send(&body).await;
    assert_eq!(response.headers["x-kaiion-mode"], "batch");
    assert_eq!(
        response.headers["x-kaiion-route-reason"],
        "reasoning_workload"
    );
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    assert_eq!(
        response.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn changed_auto_policy_cannot_resubmit_an_existing_batch_as_direct() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("jobs.db");
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let mut body = codex_request("pin");
    body["max_output_tokens"] = json!(64);
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&body).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    drop(response);
    kaiion.stop().await;
    provider.complete_all().await;
    let policy_path = directory.path().join("policy.json");
    std::fs::write(&policy_path, policy().to_string()).unwrap();
    let kaiion = start_kaiion_with_args(
        "auto",
        &database,
        fake.address,
        &["--routing-policy", policy_path.to_str().unwrap()],
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&body).await;
    assert_eq!(
        response.headers["x-kaiion-route-reason"],
        "existing_batch_job"
    );
    expect_batch_lifecycle_start(&mut response).await;
    assert_eq!(
        response.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(provider.batch_creations(), 1);
    assert!(provider.inner.direct_requests.lock().await.is_empty());
    kaiion.stop().await;
}
