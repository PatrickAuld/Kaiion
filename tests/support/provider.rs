use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::stream;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};

pub const DIRECT_SSE: &str = concat!(
    "event: response.created\n",
    "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_direct\",\"status\":\"in_progress\"}}\n\n",
    "event: response.completed\n",
    "data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_direct\",\"status\":\"completed\"}}\n\n"
);

#[derive(Clone, Default)]
pub struct FakeProvider {
    pub inner: Arc<FakeProviderInner>,
}

#[derive(Default)]
pub struct FakeProviderInner {
    files: Mutex<HashMap<String, String>>,
    batches: Mutex<HashMap<String, FakeBatch>>,
    calls: Mutex<Vec<ProviderCall>>,
    pub direct_requests: Mutex<Vec<Value>>,
    pub uploaded_batch_lines: Mutex<Vec<Value>>,
    pub batch_requests: Mutex<Vec<Value>>,
    next_file: AtomicUsize,
    next_batch: AtomicUsize,
    batch_creations: AtomicUsize,
    disconnect_create_once: AtomicBool,
    hidden_list_responses: AtomicUsize,
    malformed_batch_once: AtomicBool,
    batch_get_statuses: Mutex<VecDeque<StatusCode>>,
    file_get_statuses: Mutex<VecDeque<StatusCode>>,
    response_status: Mutex<Option<String>>,
    error_file_only: AtomicBool,
    custom_id_override: Mutex<Option<String>>,
    pagination_decoys: AtomicUsize,
    file_response_delay_ms: AtomicUsize,
}

#[derive(Clone, Debug)]
pub struct ProviderCall {
    pub path: &'static str,
    pub authorization: String,
    pub organization: Option<String>,
    pub project: Option<String>,
}

#[derive(Clone)]
struct FakeBatch {
    id: String,
    status: String,
    input_file_id: String,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
    metadata: Value,
}

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
        self.inner.calls.lock().await.push(ProviderCall {
            path,
            authorization: header_value(headers, header::AUTHORIZATION.as_str())
                .unwrap_or_default(),
            organization: header_value(headers, "openai-organization"),
            project: header_value(headers, "openai-project"),
        });
    }

    pub async fn complete_all(&self) {
        for batch in self.inner.batches.lock().await.values_mut() {
            batch.status = "completed".to_string();
        }
    }

    pub async fn set_all_batch_statuses(&self, status: &str) {
        for batch in self.inner.batches.lock().await.values_mut() {
            batch.status = status.to_string();
        }
    }

    pub async fn set_response_status(&self, status: &str) {
        *self.inner.response_status.lock().await = Some(status.to_string());
    }

    pub fn use_error_file_only(&self) {
        self.inner.error_file_only.store(true, Ordering::SeqCst);
    }

    pub async fn override_output_custom_id(&self, custom_id: &str) {
        *self.inner.custom_id_override.lock().await = Some(custom_id.to_string());
    }

    pub async fn script_batch_get_statuses(&self, statuses: impl IntoIterator<Item = StatusCode>) {
        self.inner.batch_get_statuses.lock().await.extend(statuses);
    }

    pub async fn script_file_get_statuses(&self, statuses: impl IntoIterator<Item = StatusCode>) {
        self.inner.file_get_statuses.lock().await.extend(statuses);
    }

    pub fn paginate_batch_list_after_decoys(&self, decoys: usize) {
        self.inner.pagination_decoys.store(decoys, Ordering::SeqCst);
    }

    pub fn batch_creations(&self) -> usize {
        self.inner.batch_creations.load(Ordering::SeqCst)
    }

    pub async fn calls(&self) -> Vec<ProviderCall> {
        self.inner.calls.lock().await.clone()
    }

    pub fn disconnect_next_create_after_accepting(&self) {
        self.inner
            .disconnect_create_once
            .store(true, Ordering::SeqCst);
    }

    pub fn hide_batches_for_list_calls(&self, count: usize) {
        self.inner
            .hidden_list_responses
            .store(count, Ordering::SeqCst);
    }

    pub fn malformed_next_batch_response(&self) {
        self.inner
            .malformed_batch_once
            .store(true, Ordering::SeqCst);
    }

    pub async fn seed_input_file(&self, custom_id: &str, body: Value) -> String {
        let id = format!(
            "file-seeded-{}",
            self.inner.next_file.fetch_add(1, Ordering::SeqCst)
        );
        let line = json!({
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/responses",
            "body": body
        });
        self.inner
            .files
            .lock()
            .await
            .insert(id.clone(), format!("{line}\n"));
        id
    }

    pub async fn seed_completed_batch(&self, job_id: &str, custom_id: &str) -> String {
        let output_file_id = format!(
            "file-output-seeded-{}",
            self.inner.next_file.fetch_add(1, Ordering::SeqCst)
        );
        let output = json!({
            "custom_id": custom_id,
            "response": {
                "status_code": 200,
                "body": complete_response("gpt-test", "seeded batch response")
            },
            "error": null
        });
        self.inner
            .files
            .lock()
            .await
            .insert(output_file_id.clone(), format!("{output}\n"));
        let id = format!(
            "batch-seeded-{}",
            self.inner.next_batch.fetch_add(1, Ordering::SeqCst)
        );
        self.inner.batches.lock().await.insert(
            id.clone(),
            FakeBatch {
                id: id.clone(),
                status: "completed".to_string(),
                input_file_id: "file-seeded".to_string(),
                output_file_id: Some(output_file_id),
                error_file_id: None,
                metadata: json!({"kaiion_job_id": job_id}),
            },
        );
        id
    }

    pub fn delay_file_responses(&self, milliseconds: usize) {
        self.inner
            .file_response_delay_ms
            .store(milliseconds, Ordering::SeqCst);
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
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
    provider.inner.direct_requests.lock().await.push(request);
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
) -> Result<Response, (StatusCode, String)> {
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
    let output_custom_id = provider
        .inner
        .custom_id_override
        .lock()
        .await
        .clone()
        .unwrap_or_else(|| custom_id.to_string());
    let response_status = provider
        .inner
        .response_status
        .lock()
        .await
        .clone()
        .unwrap_or_else(|| "completed".to_string());
    let mut response_body = complete_response(model, "batch response");
    response_body["status"] = Value::String(response_status);
    let output = json!({
        "id": "batch-request-1",
        "custom_id": output_custom_id,
        "response": {
            "status_code": 200,
            "request_id": "request-1",
            "body": response_body
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
    let error_file_only = provider.inner.error_file_only.load(Ordering::SeqCst);
    let error_file_id = if error_file_only {
        let id = format!(
            "file-error-{}",
            provider.inner.next_file.fetch_add(1, Ordering::SeqCst)
        );
        let error = json!({
            "custom_id": custom_id,
            "response": null,
            "error": {"code": "invalid_request", "message": "scripted error"}
        });
        provider
            .inner
            .files
            .lock()
            .await
            .insert(id.clone(), format!("{error}\n"));
        Some(id)
    } else {
        None
    };
    let batch = FakeBatch {
        id: batch_id.clone(),
        status: "in_progress".to_string(),
        input_file_id,
        output_file_id: (!error_file_only).then_some(output_file_id),
        error_file_id,
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
    if provider
        .inner
        .disconnect_create_once
        .swap(false, Ordering::SeqCst)
    {
        let body = Body::from_stream(stream::once(async {
            Err::<Bytes, std::io::Error>(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected create response disconnect",
            ))
        }));
        return Ok(Response::new(body));
    }
    Ok(Json(batch_json(&batch)).into_response())
}

async fn fake_list_batches(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    provider.record_call("batches:list", &headers).await;
    let hidden = provider
        .inner
        .hidden_list_responses
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_sub(1)
        })
        .is_ok();
    let decoys = provider.inner.pagination_decoys.load(Ordering::SeqCst);
    if !hidden && decoys > 0 && !query.contains_key("after") {
        let data = (0..decoys.min(100))
            .map(|index| {
                json!({
                    "id": format!("batch-decoy-{index}"),
                    "status": "completed",
                    "metadata": {"kaiion_job_id": format!("decoy-{index}")}
                })
            })
            .collect::<Vec<_>>();
        return Json(json!({
            "object": "list",
            "data": data,
            "has_more": true,
            "last_id": "decoy-page"
        }));
    }
    let batches = if hidden {
        Vec::new()
    } else {
        provider
            .inner
            .batches
            .lock()
            .await
            .values()
            .map(batch_json)
            .collect::<Vec<_>>()
    };
    Json(json!({"object": "list", "data": batches, "has_more": false}))
}

async fn fake_get_batch(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, StatusCode> {
    provider.record_call("batches:get", &headers).await;
    if let Some(status) = provider.inner.batch_get_statuses.lock().await.pop_front() {
        return Ok((status, Json(json!({"error": "scripted batch status"}))).into_response());
    }
    if provider
        .inner
        .malformed_batch_once
        .swap(false, Ordering::SeqCst)
    {
        return Ok(Response::new(Body::from("{not-json")));
    }
    provider
        .inner
        .batches
        .lock()
        .await
        .get(&id)
        .map(|batch| Json(batch_json(batch)).into_response())
        .ok_or(StatusCode::NOT_FOUND)
}

async fn fake_file_content(
    State(provider): State<FakeProvider>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, StatusCode> {
    provider.record_call("files:content", &headers).await;
    if let Some(status) = provider.inner.file_get_statuses.lock().await.pop_front() {
        return Ok((status, "scripted file status").into_response());
    }
    let content = provider
        .inner
        .files
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    let delay = provider.inner.file_response_delay_ms.load(Ordering::SeqCst);
    if delay > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay as u64)).await;
    }
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
        "error_file_id": batch.error_file_id,
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
            "content": [{"type": "output_text", "text": text, "annotations": []}]
        }]
    })
}

fn internal_test_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

pub struct RunningServer {
    pub address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn spawn_fake_provider(provider: FakeProvider) -> RunningServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, provider.router()).await.unwrap();
    });
    RunningServer { address, task }
}
