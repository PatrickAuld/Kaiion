#[path = "scenarios/auto_routing.rs"]
mod auto_routing;
#[path = "scenarios/durable_jobs.rs"]
mod durable_jobs;
mod support;

use std::time::Duration;

use axum::http::{StatusCode, header};
use kaiion::{
    db::Database,
    domain::{BatchId, FileId, JobState},
    request::{NormalizedRequest, UpstreamAuth},
};
use serde_json::Value;
use tempfile::TempDir;

use support::*;

async fn assert_all_calls_use_credentials(
    provider: &FakeProvider,
    api_key: &str,
    organization: Option<&str>,
    project: Option<&str>,
) {
    let calls = provider.calls().await;
    assert!(!calls.is_empty(), "expected at least one provider call");
    for call in calls {
        assert_eq!(call.authorization, format!("Bearer {api_key}"), "{call:?}");
        assert_eq!(call.organization.as_deref(), organization, "{call:?}");
        assert_eq!(call.project.as_deref(), project, "{call:?}");
    }
}

async fn verify_restart_from_state(state_name: &str) {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join(format!("{state_name}.db"));
    let database_url = format!("sqlite://{}?mode=rwc", database.display());
    let provider_url = format!("http://{}/v1", fake.address);
    let request = codex_request(state_name);
    let normalized = NormalizedRequest::from_body(&request, &provider_url).unwrap();
    let auth = UpstreamAuth {
        authorization: "Bearer test-key".to_string(),
        organization: None,
        project: None,
    };
    let db = Database::connect(&database_url).await.unwrap();
    let mut job = db
        .get_or_create(
            &auth.fingerprint(),
            &normalized.request_hash,
            &normalized.model,
        )
        .await
        .unwrap();
    if state_name == "queued" {
        provider
            .seed_input_file(&job.custom_id(), normalized.batch_body.clone())
            .await;
    }
    if state_name != "queued" {
        let file_id = provider
            .seed_input_file(&job.custom_id(), normalized.batch_body.clone())
            .await;
        job.state = db.mark_uploaded(&job.id, FileId(file_id)).await.unwrap();
    }
    if matches!(
        state_name,
        "submitting" | "submission_uncertain" | "submitted"
    ) {
        let JobState::Uploaded { input_file_id } = &job.state else {
            unreachable!()
        };
        job.state = db.begin_submission(&job.id, input_file_id).await.unwrap();
    }
    if state_name == "submission_uncertain" {
        job.state = db.mark_submission_uncertain(&job.id).await.unwrap();
    }
    if matches!(
        state_name,
        "submitting" | "submission_uncertain" | "submitted"
    ) {
        let batch_id = provider
            .seed_completed_batch(&job.id.0, &job.custom_id())
            .await;
        if state_name == "submitted" {
            job.state = db
                .mark_submitted(&job.id, &job.state, BatchId(batch_id))
                .await
                .unwrap();
        }
    }
    drop(db);

    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&request).await;
    expect_batch_lifecycle_start(&mut response).await;
    if matches!(state_name, "queued" | "uploaded") {
        wait_for_batch(&provider).await;
        provider.complete_all().await;
    }
    let events = response.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.completed");
    assert_eq!(
        provider.batch_creations(),
        usize::from(matches!(state_name, "queued" | "uploaded"))
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn direct_mode_passes_through_exact_body_sse_headers_and_authorization() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("direct", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let request = codex_request("window-1");
    let response = codex
        .send_with_headers(
            &request,
            "direct-key",
            Some("org-direct"),
            Some("project-direct"),
        )
        .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers["x-kaiion-mode"], "direct");
    assert_eq!(response.headers["x-provider-trace"], "direct-sentinel");
    assert_eq!(response.headers[header::CONTENT_TYPE], "text/event-stream");
    assert_eq!(response.all_bytes().await, DIRECT_SSE.as_bytes());
    assert_eq!(provider.batch_creations(), 0);
    assert_eq!(
        provider.inner.direct_requests.lock().await.as_slice(),
        &[request]
    );
    assert_all_calls_use_credentials(
        &provider,
        "direct-key",
        Some("org-direct"),
        Some("project-direct"),
    )
    .await;
    kaiion.stop().await;
}

#[tokio::test]
async fn batch_mode_validates_its_supported_request_contract() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);

    let mut missing_session = codex_request("missing-session");
    missing_session
        .as_object_mut()
        .unwrap()
        .remove("client_metadata");
    let response = codex.send(&missing_session).await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8(response.all_bytes().await)
            .unwrap()
            .contains("client_metadata.thread_id")
    );

    for (field, value, message) in [
        ("stream", Value::String("true".into()), "boolean"),
        ("store", Value::Bool(true), "store=true"),
        (
            "previous_response_id",
            Value::String("resp_previous".to_string()),
            "previous_response_id",
        ),
    ] {
        let mut request = codex_request(field);
        request[field] = value;
        let response = codex.send(&request).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert!(
            String::from_utf8(response.all_bytes().await)
                .unwrap()
                .contains(message)
        );
    }
    assert!(provider.calls().await.is_empty());
    kaiion.stop().await;
}

#[tokio::test]
async fn api_rejects_missing_authorization_and_invalid_mode() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/responses", kaiion.address);
    let missing = client
        .post(&url)
        .json(&codex_request("missing-auth"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    let invalid = client
        .post(&url)
        .bearer_auth("test-key")
        .header("x-kaiion-mode", "automatic")
        .json(&codex_request("invalid-mode"))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert!(provider.calls().await.is_empty());
    kaiion.stop().await;
}

#[tokio::test]
async fn batch_mode_emits_progress_and_translates_the_result_to_sse() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let request = codex_request("window-1");
    let mut response = codex
        .send_with_headers(
            &request,
            "batch-key",
            Some("org-batch"),
            Some("project-batch"),
        )
        .await;
    assert_eq!(response.status, StatusCode::OK);
    let response_id = expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    let events = response.through_terminal().await;
    assert!(events_contain(&events, "batch response"), "{events:?}");
    assert_eq!(events.last().unwrap().kind, "response.completed");
    let mut last_sequence = 1;
    for event in &events {
        let sequence = event.data["sequence_number"].as_u64().unwrap();
        assert!(sequence > last_sequence);
        last_sequence = sequence;
    }
    assert_eq!(
        events.last().unwrap().data["response"]["id"].as_str(),
        Some(response_id.as_str())
    );
    assert_eq!(provider.batch_creations(), 1);
    let uploaded = provider.inner.uploaded_batch_lines.lock().await;
    assert_eq!(uploaded.len(), 1);
    assert_eq!(uploaded[0]["body"]["stream"], false);
    assert!(uploaded[0]["body"].get("stream_options").is_none());
    drop(uploaded);
    assert_all_calls_use_credentials(
        &provider,
        "batch-key",
        Some("org-batch"),
        Some("project-batch"),
    )
    .await;
    kaiion.stop().await;
}

#[tokio::test]
async fn simultaneous_identical_requests_create_one_upstream_batch() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let request = codex_request("concurrent");
    let (mut first, mut second) = tokio::join!(codex.send(&request), codex.send(&request));
    let first_id = first.headers["x-kaiion-job-id"].clone();
    assert_eq!(second.headers["x-kaiion-job-id"], first_id);
    expect_batch_lifecycle_start(&mut first).await;
    expect_batch_lifecycle_start(&mut second).await;
    wait_for_batch(&provider).await;
    assert_eq!(provider.batch_creations(), 1);
    provider.complete_all().await;
    assert_eq!(
        first.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(
        second.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn accepted_create_with_lost_response_never_creates_a_second_batch() {
    let provider = FakeProvider::default();
    provider.disconnect_next_create_after_accepting();
    provider.hide_batches_for_list_calls(2);
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("kaiion.db");
    let first_process = start_kaiion("batch", &database, fake.address).await;
    let first_codex = FakeCodex::new(first_process.address);
    let mut stream = first_codex.send(&codex_request("ambiguous")).await;
    expect_batch_lifecycle_start(&mut stream).await;
    wait_for_batch(&provider).await;
    drop(stream);
    first_process.stop().await;

    let second_process = start_kaiion("batch", &database, fake.address).await;
    let second_codex = FakeCodex::new(second_process.address);
    let mut recovered = second_codex.send(&codex_request("ambiguous-retry")).await;
    expect_batch_lifecycle_start(&mut recovered).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(provider.batch_creations(), 1);
    provider.complete_all().await;
    assert_eq!(
        recovered.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(provider.batch_creations(), 1);
    second_process.stop().await;
}

#[tokio::test]
async fn malformed_successful_provider_json_fails_durably() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("kaiion.db");
    let kaiion = start_kaiion("batch", &database, fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let request = codex_request("malformed");
    let mut response = codex.send(&request).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.malformed_next_batch_response();
    let events = response.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.failed");
    assert!(events_contain(&events, "protocol violation"));
    let calls = provider.calls().await.len();

    let mut replay = codex.send(&request).await;
    let replayed = replay.through_terminal().await;
    assert_eq!(replayed.last().unwrap().kind, "response.failed");
    assert_eq!(provider.calls().await.len(), calls);
    kaiion.stop().await;
}

#[tokio::test]
async fn response_failed_and_incomplete_statuses_are_preserved() {
    for (status, expected_event) in [
        ("failed", "response.failed"),
        ("incomplete", "response.incomplete"),
    ] {
        let provider = FakeProvider::default();
        provider.set_response_status(status).await;
        let fake = spawn_fake_provider(provider.clone()).await;
        let directory = TempDir::new().unwrap();
        let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
        let codex = FakeCodex::new(kaiion.address);
        let mut response = codex.send(&codex_request(status)).await;
        expect_batch_lifecycle_start(&mut response).await;
        wait_for_batch(&provider).await;
        provider.complete_all().await;
        let events = response.through_terminal().await;
        assert_eq!(events.last().unwrap().kind, expected_event);
        assert_eq!(events.last().unwrap().data["response"]["status"], status);
        kaiion.stop().await;
    }
}

#[tokio::test]
async fn terminal_batch_statuses_and_error_only_completion_fail_durably() {
    for status in ["failed", "expired", "cancelled"] {
        let provider = FakeProvider::default();
        let fake = spawn_fake_provider(provider.clone()).await;
        let directory = TempDir::new().unwrap();
        let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
        let codex = FakeCodex::new(kaiion.address);
        let mut response = codex.send(&codex_request(status)).await;
        expect_batch_lifecycle_start(&mut response).await;
        wait_for_batch(&provider).await;
        provider.set_all_batch_statuses(status).await;
        assert_eq!(
            response.through_terminal().await.last().unwrap().kind,
            "response.failed"
        );
        kaiion.stop().await;
    }

    let provider = FakeProvider::default();
    provider.use_error_file_only();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&codex_request("error-only")).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    let events = response.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.failed");
    assert!(events_contain(&events, "scripted error"));
    kaiion.stop().await;
}

#[tokio::test]
async fn retryable_batch_and_output_visibility_errors_recover() {
    let provider = FakeProvider::default();
    provider
        .script_batch_get_statuses([
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
        ])
        .await;
    provider
        .script_file_get_statuses([StatusCode::NOT_FOUND, StatusCode::CONFLICT])
        .await;
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&codex_request("retryable-errors")).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    assert_eq!(
        response.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(provider.batch_creations(), 1);
    kaiion.stop().await;
}

#[tokio::test]
async fn permanent_http_and_wrong_custom_id_fail_durably() {
    let provider = FakeProvider::default();
    provider
        .script_batch_get_statuses([StatusCode::BAD_REQUEST])
        .await;
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("http.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&codex_request("permanent-http")).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    assert_eq!(
        response.through_terminal().await.last().unwrap().kind,
        "response.failed"
    );
    kaiion.stop().await;

    let provider = FakeProvider::default();
    provider.override_output_custom_id("wrong-id").await;
    let fake = spawn_fake_provider(provider.clone()).await;
    let kaiion = start_kaiion(
        "batch",
        &directory.path().join("custom-id.db"),
        fake.address,
    )
    .await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&codex_request("wrong-id")).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    let events = response.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.failed");
    assert!(events_contain(&events, "does not contain custom_id"));
    kaiion.stop().await;
}

#[tokio::test]
async fn uncertain_submission_reconciliation_follows_batch_list_pagination() {
    let provider = FakeProvider::default();
    provider.disconnect_next_create_after_accepting();
    provider.paginate_batch_list_after_decoys(100);
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&codex_request("pagination")).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    assert_eq!(
        response.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(provider.batch_creations(), 1);
    let list_calls = provider
        .calls()
        .await
        .iter()
        .filter(|call| call.path == "batches:list")
        .count();
    assert!(list_calls >= 2);
    kaiion.stop().await;
}

#[tokio::test]
async fn client_disconnect_and_process_restart_replay_one_durable_result() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("kaiion.db");
    let first_process = start_kaiion("batch", &database, fake.address).await;
    let first_codex = FakeCodex::new(first_process.address);
    let request = codex_request("before");
    let mut first = first_codex.send(&request).await;
    let job_id = first.headers["x-kaiion-job-id"].clone();
    expect_batch_lifecycle_start(&mut first).await;
    wait_for_batch(&provider).await;
    drop(first);
    first_process.stop().await;

    provider.complete_all().await;
    let second_process = start_kaiion("batch", &database, fake.address).await;
    let second_codex = FakeCodex::new(second_process.address);
    let mut recovered = second_codex.send(&request).await;
    assert_eq!(recovered.headers["x-kaiion-job-id"], job_id);
    let events = recovered.through_terminal().await;
    assert!(events_contain(&events, "batch response"));
    assert_eq!(provider.batch_creations(), 1);
    let calls = provider.calls().await.len();

    let mut replay = second_codex.send(&request).await;
    assert_eq!(
        replay.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(provider.calls().await.len(), calls);
    second_process.stop().await;
}

#[tokio::test]
async fn restart_is_deterministic_from_every_persisted_nonterminal_state() {
    for state in [
        "queued",
        "uploaded",
        "submitting",
        "submission_uncertain",
        "submitted",
    ] {
        verify_restart_from_state(state).await;
    }
}

#[tokio::test]
async fn crash_during_output_retrieval_replays_and_persists_the_result() {
    let provider = FakeProvider::default();
    provider.delay_file_responses(2000);
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("kaiion.db");
    let request = codex_request("output-crash");
    let first_process = start_kaiion("batch", &database, fake.address).await;
    let first_codex = FakeCodex::new(first_process.address);
    let mut first = first_codex.send(&request).await;
    expect_batch_lifecycle_start(&mut first).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    wait_for_provider_call(&provider, "files:content").await;
    first_process.stop().await;
    drop(first);

    provider.delay_file_responses(0);
    let second_process = start_kaiion("batch", &database, fake.address).await;
    let second_codex = FakeCodex::new(second_process.address);
    let mut recovered = second_codex.send(&request).await;
    let events = recovered.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.completed");
    assert!(events_contain(&events, "batch response"));
    assert_eq!(provider.batch_creations(), 1);
    let calls = provider.calls().await.len();
    let mut replay = second_codex.send(&request).await;
    assert_eq!(
        replay.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(provider.calls().await.len(), calls);
    second_process.stop().await;
}

#[tokio::test]
async fn credentials_scope_durable_request_identity() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let request = codex_request("isolation");
    let mut first = codex
        .send_with_headers(&request, "tenant-a", None, None)
        .await;
    let mut second = codex
        .send_with_headers(&request, "tenant-b", None, None)
        .await;
    expect_batch_lifecycle_start(&mut first).await;
    expect_batch_lifecycle_start(&mut second).await;
    wait_for_batch_count(&provider, 2).await;
    assert_ne!(
        first.headers["x-kaiion-job-id"],
        second.headers["x-kaiion-job-id"]
    );
    provider.complete_all().await;
    assert_eq!(
        first.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    assert_eq!(
        second.through_terminal().await.last().unwrap().kind,
        "response.completed"
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn terminal_notification_is_not_delayed_until_the_next_heartbeat() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut response = codex.send(&codex_request("notification")).await;
    expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    provider.complete_all().await;
    wait_for_provider_call(&provider, "files:content").await;
    let started = std::time::Instant::now();
    let events = response.through_terminal().await;
    assert_eq!(events.last().unwrap().kind, "response.completed");
    assert!(started.elapsed() < Duration::from_millis(500));
    let client = reqwest::Client::new();
    for _ in 0..20 {
        let health: Value = client
            .get(format!("http://{}/healthz", kaiion.address))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if health["active_batch_workers"] == 0 {
            kaiion.stop().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("terminal worker was not evicted");
}
