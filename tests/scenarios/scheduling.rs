use std::time::{Duration, Instant};

use axum::http::StatusCode;
use futures_util::future::join_all;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::support::process::start_kaiion_with_args;
use crate::support::*;

fn policy(limits: Value, max_retries: u64) -> Value {
    json!({
        "enabled": true,
        "limits": limits,
        "max_wait_ms": 10_000,
        "max_retries": max_retries,
        "queue_coalesce_ms": 2
    })
}

async fn start_scheduled(
    mode: &str,
    directory: &TempDir,
    fake_provider: std::net::SocketAddr,
    policy: &Value,
) -> crate::support::process::KaiionProcess {
    let policy_path = directory.path().join("scheduling.json");
    std::fs::write(&policy_path, policy.to_string()).unwrap();
    let policy_path = policy_path.to_string_lossy().into_owned();
    start_kaiion_with_args(
        mode,
        &directory.path().join("kaiion.db"),
        fake_provider,
        &["--scheduling-policy", policy_path.as_str()],
    )
    .await
}

fn request_for(model: &str, id: &str) -> Value {
    let mut request = codex_request(id);
    request["model"] = Value::String(model.to_string());
    request["client_metadata"]["turn_id"] = Value::String(id.to_string());
    request
}

fn request_for_session(model: &str, id: &str, session: &str) -> Value {
    let mut request = request_for(model, id);
    request["client_metadata"]["session_id"] = Value::String(session.to_string());
    request["client_metadata"]["thread_id"] = Value::String(session.to_string());
    request
}

async fn send_concurrently(codex: &FakeCodex, requests: &[Value]) {
    let responses = join_all(requests.iter().map(|request| codex.send(request))).await;
    for response in responses {
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    }
}

fn assert_minimum_gaps(offsets: &[Duration], minimum: Duration) {
    for gap in offsets.windows(2).map(|pair| pair[1] - pair[0]) {
        assert!(
            gap >= minimum,
            "requests arrived only {gap:?} apart; expected at least {minimum:?}: {offsets:?}"
        );
    }
}

#[tokio::test]
async fn rolling_request_quota_spreads_concurrent_direct_requests_over_the_window() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled(
        "direct",
        &directory,
        fake.address,
        &policy(json!([{"window_seconds": 1, "max_rps": 2}]), 0),
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let requests = (0..3)
        .map(|index| request_for("gpt-test", &format!("quota-{index}")))
        .collect::<Vec<_>>();

    send_concurrently(&codex, &requests).await;

    let observations = provider.direct_observations().await;
    assert_eq!(observations.len(), 3);
    let offsets = observations
        .iter()
        .map(|observation| observation.offset)
        .collect::<Vec<_>>();
    assert_minimum_gaps(&offsets, Duration::from_millis(400));
    assert!(offsets[2] >= Duration::from_millis(800), "{offsets:?}");
    kaiion.stop().await;
}

#[tokio::test]
async fn provider_wide_and_per_model_request_quotas_compose() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled(
        "direct",
        &directory,
        fake.address,
        &policy(
            json!([
                {"window_seconds": 1, "max_rps": 2},
                {"model": "gpt-test", "window_seconds": 1, "max_rps": 1}
            ]),
            0,
        ),
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let requests = [
        request_for("gpt-test", "mixed-0"),
        request_for("gpt-other", "mixed-1"),
        request_for("gpt-test", "mixed-2"),
        request_for("gpt-other", "mixed-3"),
    ];

    send_concurrently(&codex, &requests).await;

    let observations = provider.direct_observations().await;
    assert_eq!(observations.len(), 4);
    let offsets = observations
        .iter()
        .map(|observation| observation.offset)
        .collect::<Vec<_>>();
    assert_minimum_gaps(&offsets, Duration::from_millis(400));
    let gpt_test_offsets = observations
        .iter()
        .filter(|observation| observation.request["model"] == "gpt-test")
        .map(|observation| observation.offset)
        .collect::<Vec<_>>();
    assert_eq!(gpt_test_offsets.len(), 2);
    assert!(
        gpt_test_offsets[1] - gpt_test_offsets[0] >= Duration::from_millis(900),
        "per-model requests were not limited in addition to provider-wide quota: {observations:?}"
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn rolling_request_count_limit_is_distinct_from_smoothed_rps() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled(
        "direct",
        &directory,
        fake.address,
        &policy(json!([{"window_seconds": 1, "max_requests": 2}]), 0),
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let requests = (0..3)
        .map(|index| request_for("gpt-test", &format!("rolling-{index}")))
        .collect::<Vec<_>>();

    send_concurrently(&codex, &requests).await;

    let offsets = provider.direct_request_offsets().await;
    assert_eq!(offsets.len(), 3);
    assert!(
        offsets[2] - offsets[0] >= Duration::from_millis(850),
        "{offsets:?}"
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn provider_wide_quota_aggregates_requests_across_api_keys() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled(
        "direct",
        &directory,
        fake.address,
        &policy(json!([{"window_seconds": 1, "max_rps": 2}]), 0),
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let first_request = request_for("gpt-test", "key-a");
    let second_request = request_for("gpt-test", "key-b");
    let (first, second) = tokio::join!(
        codex.send_with_headers(&first_request, "key-a", None, None),
        codex.send_with_headers(&second_request, "key-b", None, None)
    );
    for response in [first, second] {
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    }

    let offsets = provider.direct_request_offsets().await;
    assert_eq!(offsets.len(), 2);
    assert_minimum_gaps(&offsets, Duration::from_millis(400));
    let calls = provider.calls().await;
    assert!(
        calls
            .iter()
            .any(|call| call.authorization == "Bearer key-a")
    );
    assert!(
        calls
            .iter()
            .any(|call| call.authorization == "Bearer key-b")
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn quick_user_followup_is_dispatched_before_an_async_alias_waiting_in_queue() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let mut schedule = policy(json!([{"window_seconds": 1, "max_rps": 1}]), 0);
    schedule["queue_coalesce_ms"] = json!(50);
    let kaiion = start_scheduled("direct", &directory, fake.address, &schedule).await;
    let codex = FakeCodex::new(kaiion.address);

    let first = request_for_session("gpt-test", "fast-first", "session-fast");
    let response = codex.send(&first).await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());

    let slow_alias = request_for_session("async-gpt-test", "async-wait", "session-slow");
    let fast_followup = request_for_session("gpt-test", "fast-followup", "session-fast");
    let (slow_response, fast_response) =
        tokio::join!(codex.send(&slow_alias), codex.send(&fast_followup));
    for response in [slow_response, fast_response] {
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    }

    let observations = provider.direct_observations().await;
    assert_eq!(observations.len(), 3);
    assert_eq!(
        observations[1].request["client_metadata"]["turn_id"], "fast-followup",
        "the prompt quickly following a turn in its session should outrank the wait-tolerant alias"
    );
    assert_eq!(
        observations[2].request["client_metadata"]["turn_id"],
        "async-wait"
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn omitted_output_limit_uses_the_conservative_token_reservation() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled(
        "direct",
        &directory,
        fake.address,
        &policy(
            json!([{
                "window_seconds": 1,
                "max_output_tokens": 100
            }]),
            0,
        ),
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let response = codex
        .send(&request_for("gpt-test", "conservative-output"))
        .await;
    assert_ne!(response.status, StatusCode::OK);
    let _ = response.all_bytes().await;
    assert!(provider.inner.direct_requests.lock().await.is_empty());
    kaiion.stop().await;
}

#[tokio::test]
async fn fake_provider_records_concurrent_live_request_arrivals() {
    let provider = FakeProvider::default();
    provider.delay_direct_responses(250);
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("direct", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let requests = (0..3)
        .map(|index| request_for("gpt-test", &format!("concurrent-{index}")))
        .collect::<Vec<_>>();

    send_concurrently(&codex, &requests).await;

    assert_eq!(provider.direct_observations().await.len(), 3);
    assert!(provider.max_concurrent_direct_requests() >= 2);
    kaiion.stop().await;
}

#[tokio::test]
async fn direct_requests_retry_429_after_the_retry_after_delay() {
    let provider = FakeProvider::default();
    provider
        .script_direct_responses([(StatusCode::TOO_MANY_REQUESTS, Some("1".to_string()))])
        .await;
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled("direct", &directory, fake.address, &policy(json!([]), 1)).await;
    let codex = FakeCodex::new(kaiion.address);
    let started = Instant::now();
    let response = codex.send(&request_for("gpt-test", "retry-429")).await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    assert!(started.elapsed() >= Duration::from_millis(850));
    assert_eq!(provider.inner.direct_requests.lock().await.len(), 2);
    kaiion.stop().await;
}

#[tokio::test]
async fn async_model_alias_is_removed_before_live_upstream_request() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_scheduled(
        "direct",
        &directory,
        fake.address,
        &policy(json!([{"window_seconds": 1, "max_rps": 10}]), 0),
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let response = codex
        .send(&request_for("async-gpt-test", "async-alias"))
        .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    let requests = provider.inner.direct_requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["model"], "gpt-test");
    kaiion.stop().await;
}

#[tokio::test]
async fn model_listing_exposes_async_aliases_without_double_prefixing() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("direct", &directory.path().join("kaiion.db"), fake.address).await;
    let response = reqwest::Client::new()
        .get(format!("http://{}/v1/models", kaiion.address))
        .bearer_auth("models-key")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models: Value = response.json().await.unwrap();
    let ids = models["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|model| model["id"].as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&"gpt-test"));
    assert!(ids.contains(&"async-gpt-test"));
    assert!(ids.contains(&"async-provider-model"));
    assert!(!ids.contains(&"async-async-provider-model"));
    assert_eq!(ids.iter().filter(|id| **id == "async-gpt-test").count(), 1);
    assert!(
        provider
            .calls()
            .await
            .iter()
            .any(|call| call.path == "models" && call.authorization == "Bearer models-key")
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn empty_async_alias_is_rejected_before_provider_call() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("direct", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let response = codex.send(&request_for("async-", "empty-alias")).await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8(response.all_bytes().await)
            .unwrap()
            .contains("async- model alias must include an upstream model name")
    );
    assert!(provider.calls().await.is_empty());
    kaiion.stop().await;
}

#[tokio::test]
async fn batch_submission_strips_async_alias_from_uploaded_request() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex
        .send(&request_for("async-gpt-test", "batch-alias"))
        .await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    let uploaded = provider.inner.uploaded_batch_lines.lock().await;
    assert_eq!(uploaded.len(), 1);
    assert_eq!(uploaded[0]["body"]["model"], "gpt-test");
    drop(uploaded);
    kaiion.stop().await;
}
