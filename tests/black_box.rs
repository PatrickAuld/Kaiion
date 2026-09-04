use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Multipart, Path as AxumPath, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    sync::Mutex,
    task::JoinHandle,
};

#[derive(Clone, Default)]
struct FakeProvider {
    inner: Arc<FakeProviderInner>,
}

#[derive(Default)]
struct FakeProviderInner {
    files: Mutex<HashMap<String, String>>,
    batches: Mutex<HashMap<String, FakeBatch>>,
    calls: Mutex<Vec<ProviderCall>>,
    direct_requests: Mutex<Vec<Value>>,
    uploaded_batch_lines: Mutex<Vec<Value>>,
    batch_requests: Mutex<Vec<Value>>,
    next_file: AtomicUsize,
    next_batch: AtomicUsize,
    batch_creations: AtomicUsize,
}

#[derive(Clone, Debug)]
struct ProviderCall {
    path: &'static str,
    authorization: String,
    organization: Option<String>,
    project: Option<String>,
}

#[derive(Clone)]
struct FakeBatch {
    id: String,
    status: String,
    input_file_id: String,
    output_file_id: String,
    metadata: Value,
}

const DIRECT_SSE: &str = concat!(
    "event: response.created\n",
    "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_direct\",\"status\":\"in_progress\"}}\n\n",
    "event: response.completed\n",
    "data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_direct\",\"status\":\"completed\"}}\n\n"
);

impl FakeProvider {
    fn router(self) -> Router {
        Router::new()
            .route("/v1/responses", post(fake_direct_response))
            .route("/v1/files", post(fake_upload_file))
            .route("/v1/files/{id}/content", get(fake_file_content))
            .route(
                "/v1/batches",
                post(fake_create_batch).get(fake_list_batches),
            )
            .route("/v1/batches/{id}", get(fake_get_batch))
            .with_state(self)
    }

    async fn record_call(&self, path: &'static str, headers: &HeaderMap) {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let organization = header_value(headers, "openai-organization");
        let project = header_value(headers, "openai-project");
        self.inner.calls.lock().await.push(ProviderCall {
            path,
            authorization,
            organization,
            project,
        });
    }

    async fn complete_all(&self) {
        for batch in self.inner.batches.lock().await.values_mut() {
            batch.status = "completed".to_string();
        }
    }

    fn batch_creations(&self) -> usize {
        self.inner.batch_creations.load(Ordering::SeqCst)
    }

    async fn calls(&self) -> Vec<ProviderCall> {
        self.inner.calls.lock().await.clone()
    }
}

fn header_value(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn fake_direct_response(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    provider.record_call("responses", &headers).await;
    provider
        .inner
        .direct_requests
        .lock()
        .await
        .push(request.clone());
    let mut response = Response::new(Body::from(DIRECT_SSE));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        "x-provider-trace",
        HeaderValue::from_static("direct-sentinel"),
    );
    response
}

async fn fake_upload_file(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<Value>, (StatusCode, String)> {
    provider.record_call("files", &headers).await;
    let mut content = None;
    while let Some(field) = multipart.next_field().await.map_err(internal_test_error)? {
        if field.name() == Some("file") {
            content = Some(field.text().await.map_err(internal_test_error)?);
        }
    }
    let content = content.ok_or((StatusCode::BAD_REQUEST, "missing file".to_string()))?;
    let lines = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(internal_test_error))
        .collect::<Result<Vec<Value>, _>>()?;
    if lines.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "batch input is empty".to_string()));
    }
    provider
        .inner
        .uploaded_batch_lines
        .lock()
        .await
        .extend(lines);
    let number = provider.inner.next_file.fetch_add(1, Ordering::SeqCst);
    let id = format!("file-input-{number}");
    provider
        .inner
        .files
        .lock()
        .await
        .insert(id.clone(), content);
    Ok(Json(json!({"id": id, "object": "file"})))
}

async fn fake_create_batch(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<Json<Value>, (StatusCode, String)> {
    provider.record_call("batches", &headers).await;
    provider
        .inner
        .batch_requests
        .lock()
        .await
        .push(request.clone());
    let input_file_id = request["input_file_id"]
        .as_str()
        .ok_or((StatusCode::BAD_REQUEST, "missing input_file_id".to_string()))?
        .to_string();
    let input = provider
        .inner
        .files
        .lock()
        .await
        .get(&input_file_id)
        .cloned()
        .ok_or((StatusCode::NOT_FOUND, "input file not found".to_string()))?;
    let input: Value = serde_json::from_str(input.lines().next().unwrap_or_default())
        .map_err(internal_test_error)?;
    let custom_id = input["custom_id"]
        .as_str()
        .ok_or((StatusCode::BAD_REQUEST, "missing custom_id".to_string()))?;
    let model = input["body"]["model"].as_str().unwrap_or("gpt-test");
    let result = complete_response(model, "batch response");
    let output = json!({
        "id": "batch-request-1",
        "custom_id": custom_id,
        "response": {
            "status_code": 200,
            "request_id": "request-1",
            "body": result
        },
        "error": null
    });
    let output_file_id = format!(
        "file-output-{}",
        provider.inner.next_file.fetch_add(1, Ordering::SeqCst)
    );
    provider
        .inner
        .files
        .lock()
        .await
        .insert(output_file_id.clone(), format!("{output}\n"));

    let batch_id = format!(
        "batch-{}",
        provider.inner.next_batch.fetch_add(1, Ordering::SeqCst)
    );
    let batch = FakeBatch {
        id: batch_id.clone(),
        status: "in_progress".to_string(),
        input_file_id,
        output_file_id,
        metadata: request.get("metadata").cloned().unwrap_or(Value::Null),
    };
    provider
        .inner
        .batches
        .lock()
        .await
        .insert(batch_id, batch.clone());
    provider
        .inner
        .batch_creations
        .fetch_add(1, Ordering::SeqCst);
    Ok(Json(batch_json(&batch)))
}

async fn fake_list_batches(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
) -> Json<Value> {
    provider.record_call("batches:list", &headers).await;
    let batches = provider
        .inner
        .batches
        .lock()
        .await
        .values()
        .map(batch_json)
        .collect::<Vec<_>>();
    Json(json!({"object": "list", "data": batches, "has_more": false}))
}

async fn fake_get_batch(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, StatusCode> {
    provider.record_call("batches:get", &headers).await;
    provider
        .inner
        .batches
        .lock()
        .await
        .get(&id)
        .map(|batch| Json(batch_json(batch)))
        .ok_or(StatusCode::NOT_FOUND)
}

async fn fake_file_content(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, StatusCode> {
    provider.record_call("files:content", &headers).await;
    let content = provider
        .inner
        .files
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    let mut response = Response::new(Body::from(content));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/jsonl"),
    );
    Ok(response)
}

fn batch_json(batch: &FakeBatch) -> Value {
    json!({
        "id": batch.id,
        "object": "batch",
        "endpoint": "/v1/responses",
        "status": batch.status,
        "input_file_id": batch.input_file_id,
        "output_file_id": batch.output_file_id,
        "error_file_id": null,
        "metadata": batch.metadata
    })
}

fn complete_response(model: &str, text: &str) -> Value {
    json!({
        "id": "resp_fake",
        "object": "response",
        "created_at": 1,
        "status": "completed",
        "model": model,
        "output": [{
            "id": "msg_fake",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{
                "type": "output_text",
                "text": text,
                "annotations": []
            }]
        }],
        "usage": {
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 2,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 12
        }
    })
}

fn internal_test_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

struct RunningServer {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spawn_fake_provider(provider: FakeProvider) -> RunningServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, provider.router()).await.unwrap();
    });
    RunningServer { address, task }
}

struct KaiionProcess {
    child: Child,
    address: SocketAddr,
}

impl KaiionProcess {
    async fn stop(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

impl Drop for KaiionProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

async fn start_kaiion(mode: &str, database: &Path, upstream: SocketAddr) -> KaiionProcess {
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

fn codex_request(window_id: &str) -> Value {
    json!({
        "model": "gpt-test",
        "instructions": "Return a result",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }],
        "tools": [],
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "store": false,
        "stream": true,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": "session-1",
        "client_metadata": {
            "session_id": "session-1",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "x-codex-window-id": window_id,
            "x-codex-turn-metadata": format!("metadata-{window_id}")
        }
    })
}

struct FakeCodex {
    client: reqwest::Client,
    address: SocketAddr,
}

struct CodexStream {
    status: StatusCode,
    headers: HeaderMap,
    bytes: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    buffer: Vec<u8>,
}

#[derive(Debug)]
struct SseEvent {
    kind: String,
    data: Value,
}

impl FakeCodex {
    fn new(address: SocketAddr) -> Self {
        Self {
            client: reqwest::Client::new(),
            address,
        }
    }

    async fn send(&self, request: &Value) -> CodexStream {
        self.send_with_headers(request, "test-key", None, None)
            .await
    }

    async fn send_with_headers(
        &self,
        request: &Value,
        api_key: &str,
        organization: Option<&str>,
        project: Option<&str>,
    ) -> CodexStream {
        let mut outbound = self
            .client
            .post(format!("http://{}/v1/responses", self.address))
            .bearer_auth(api_key);
        if let Some(organization) = organization {
            outbound = outbound.header("openai-organization", organization);
        }
        if let Some(project) = project {
            outbound = outbound.header("openai-project", project);
        }
        let response = outbound.json(request).send().await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        CodexStream {
            status,
            headers,
            bytes: Box::pin(response.bytes_stream()),
            buffer: Vec::new(),
        }
    }
}

impl CodexStream {
    async fn all_bytes(mut self) -> Vec<u8> {
        let mut bytes = std::mem::take(&mut self.buffer);
        while let Some(next) = self.bytes.next().await {
            bytes.extend_from_slice(&next.expect("SSE transport failed"));
        }
        bytes
    }

    async fn next_event(&mut self) -> Option<SseEvent> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(event) = take_sse_event(&mut self.buffer) {
                    return Some(event);
                }
                match self.bytes.next().await {
                    Some(Ok(bytes)) => self.buffer.extend_from_slice(&bytes),
                    Some(Err(error)) => panic!("SSE transport failed: {error}"),
                    None => return take_sse_event(&mut self.buffer),
                }
            }
        })
        .await
        .expect("SSE event timed out")
    }

    async fn through_terminal(&mut self) -> Vec<SseEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.next_event().await {
            let terminal = matches!(
                event.kind.as_str(),
                "response.completed" | "response.failed"
            );
            events.push(event);
            if terminal {
                break;
            }
        }
        events
    }
}

fn take_sse_event(buffer: &mut Vec<u8>) -> Option<SseEvent> {
    let boundary = buffer.windows(2).position(|window| window == b"\n\n")?;
    let frame = buffer.drain(..boundary + 2).collect::<Vec<_>>();
    let frame = String::from_utf8(frame).expect("fake provider emitted invalid UTF-8");
    let kind = frame
        .lines()
        .find_map(|line| line.strip_prefix("event: "))?
        .to_string();
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect::<Vec<_>>()
        .join("\n");
    Some(SseEvent {
        kind,
        data: serde_json::from_str(&data).expect("fake provider emitted invalid SSE JSON"),
    })
}

fn events_contain(events: &[SseEvent], needle: &str) -> bool {
    events
        .iter()
        .any(|event| event.data.to_string().contains(needle))
}

async fn wait_for_batch(provider: &FakeProvider) {
    wait_for_batch_count(provider, 1).await;
}

async fn wait_for_batch_count(provider: &FakeProvider, expected: usize) {
    for _ in 0..400 {
        if provider.batch_creations() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Kaiion did not create {expected} batches");
}

async fn wait_for_provider_call(provider: &FakeProvider, path: &str) {
    for _ in 0..400 {
        if provider.calls().await.iter().any(|call| call.path == path) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Kaiion did not call provider endpoint {path}");
}

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

async fn expect_batch_lifecycle_start(stream: &mut CodexStream) -> String {
    let created = stream.next_event().await.unwrap();
    assert_eq!(created.kind, "response.created");
    assert_eq!(created.data["sequence_number"], 0);
    assert_eq!(created.data["response"]["status"], "in_progress");
    let response_id = created.data["response"]["id"].as_str().unwrap().to_string();

    let progress = stream.next_event().await.unwrap();
    assert_eq!(progress.kind, "response.in_progress");
    assert_eq!(progress.data["sequence_number"], 1);
    assert_eq!(
        progress.data["response"]["id"].as_str(),
        Some(response_id.as_str())
    );
    response_id
}

#[tokio::test]
async fn direct_mode_passes_through_sse_and_authorization() {
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
async fn batch_mode_rejects_requests_without_restart_safe_session_identity() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let mut request = codex_request("window-1");
    request.as_object_mut().unwrap().remove("client_metadata");

    let response = codex.send(&request).await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    let body = String::from_utf8(response.all_bytes().await).unwrap();
    assert!(body.contains("client_metadata.thread_id"));
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
    assert_eq!(response.headers["x-kaiion-mode"], "batch");
    let response_id = expect_batch_lifecycle_start(&mut response).await;
    wait_for_batch(&provider).await;
    let second_progress = response.next_event().await.unwrap();
    assert_eq!(second_progress.kind, "response.in_progress");
    assert_eq!(second_progress.data["sequence_number"], 2);
    assert_eq!(
        second_progress.data["response"]["id"].as_str(),
        Some(response_id.as_str())
    );
    provider.complete_all().await;

    let events = response.through_terminal().await;
    assert!(events_contain(&events, "batch response"));
    assert!(
        events
            .iter()
            .any(|event| event.kind == "response.output_item.done")
    );
    assert_eq!(events.last().unwrap().kind, "response.completed");
    let terminal_events = events
        .iter()
        .filter(|event| event.kind != "response.in_progress")
        .collect::<Vec<_>>();
    assert_eq!(
        terminal_events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["response.output_item.done", "response.completed"]
    );
    assert_eq!(terminal_events[0].data["sequence_number"], 3);
    assert_eq!(terminal_events[1].data["sequence_number"], 4);
    assert_eq!(
        terminal_events[1].data["response"]["id"].as_str(),
        Some(response_id.as_str())
    );
    assert_eq!(provider.batch_creations(), 1);
    let uploaded = provider.inner.uploaded_batch_lines.lock().await;
    assert_eq!(uploaded.len(), 1);
    assert_eq!(uploaded[0]["method"], "POST");
    assert_eq!(uploaded[0]["url"], "/v1/responses");
    assert_eq!(uploaded[0]["body"]["stream"], false);
    assert!(uploaded[0]["body"].get("stream_options").is_none());
    assert_eq!(uploaded[0]["body"]["input"], request["input"]);
    drop(uploaded);
    let batches = provider.inner.batch_requests.lock().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0]["endpoint"], "/v1/responses");
    assert_eq!(batches[0]["completion_window"], "24h");
    assert!(batches[0]["metadata"]["kaiion_job_id"].is_string());
    drop(batches);
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
async fn client_disconnect_does_not_cancel_the_batch_or_create_a_duplicate() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);

    let mut first = codex.send(&codex_request("window-first")).await;
    assert_eq!(first.status, StatusCode::OK);
    let job_id = first.headers["x-kaiion-job-id"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        expect_batch_lifecycle_start(&mut first)
            .await
            .starts_with("resp_kaiion_")
    );
    drop(first);
    wait_for_batch(&provider).await;

    // A reconnect with only volatile Codex fields changed must find the same durable job.
    let mut duplicate = codex.send(&codex_request("window-second")).await;
    assert_eq!(
        duplicate.headers["x-kaiion-job-id"].to_str().unwrap(),
        job_id
    );
    assert!(
        expect_batch_lifecycle_start(&mut duplicate)
            .await
            .starts_with("resp_kaiion_")
    );
    assert_eq!(provider.batch_creations(), 1);
    drop(duplicate);

    // Completion is driven by Kaiion's poller, not a still-open Codex TCP stream.
    provider.complete_all().await;
    wait_for_provider_call(&provider, "files:content").await;

    let mut replay = codex.send(&codex_request("window-third")).await;
    assert_eq!(replay.headers["x-kaiion-job-id"].to_str().unwrap(), job_id);
    let events = replay.through_terminal().await;
    assert!(events_contain(&events, "batch response"));
    assert_eq!(events.last().unwrap().kind, "response.completed");
    assert_eq!(provider.batch_creations(), 1);
    kaiion.stop().await;
}

#[tokio::test]
async fn identical_requests_with_different_api_keys_are_not_deduplicated() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let kaiion = start_kaiion("batch", &directory.path().join("kaiion.db"), fake.address).await;
    let codex = FakeCodex::new(kaiion.address);
    let request = codex_request("window-isolation");

    let mut first = codex
        .send_with_headers(&request, "tenant-a", None, None)
        .await;
    assert!(
        expect_batch_lifecycle_start(&mut first)
            .await
            .starts_with("resp_kaiion_")
    );
    wait_for_batch_count(&provider, 1).await;

    let mut second = codex
        .send_with_headers(&request, "tenant-b", None, None)
        .await;
    assert!(
        expect_batch_lifecycle_start(&mut second)
            .await
            .starts_with("resp_kaiion_")
    );
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
    let calls = provider.calls().await;
    assert!(
        calls
            .iter()
            .any(|call| call.authorization == "Bearer tenant-a")
    );
    assert!(
        calls
            .iter()
            .any(|call| call.authorization == "Bearer tenant-b")
    );
    kaiion.stop().await;
}

#[tokio::test]
async fn restart_reuses_the_batch_and_replays_its_result() {
    let provider = FakeProvider::default();
    let fake = spawn_fake_provider(provider.clone()).await;
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("kaiion.db");

    let first_process = start_kaiion("batch", &database, fake.address).await;
    let first_codex = FakeCodex::new(first_process.address);
    let mut first_stream = first_codex.send(&codex_request("window-before")).await;
    assert!(
        expect_batch_lifecycle_start(&mut first_stream)
            .await
            .starts_with("resp_kaiion_")
    );
    wait_for_batch(&provider).await;
    drop(first_stream);
    first_process.stop().await;

    provider.complete_all().await;
    let second_process = start_kaiion("batch", &database, fake.address).await;
    let second_codex = FakeCodex::new(second_process.address);
    let mut response = second_codex.send(&codex_request("window-after")).await;
    let events = response.through_terminal().await;

    assert!(events_contain(&events, "batch response"));
    assert_eq!(events.last().unwrap().kind, "response.completed");
    assert_eq!(provider.batch_creations(), 1);
    assert_all_calls_use_credentials(&provider, "test-key", None, None).await;

    let upstream_calls = provider.calls().await.len();
    second_process.stop().await;
    let third_process = start_kaiion("batch", &database, fake.address).await;
    let third_codex = FakeCodex::new(third_process.address);
    let mut replay = third_codex.send(&codex_request("window-replay")).await;
    let replayed_events = replay.through_terminal().await;
    assert!(events_contain(&replayed_events, "batch response"));
    assert_eq!(replayed_events.last().unwrap().kind, "response.completed");
    assert_eq!(provider.batch_creations(), 1);
    assert_eq!(provider.calls().await.len(), upstream_calls);
    third_process.stop().await;
}
