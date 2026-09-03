use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
};

use async_stream::stream;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};
use tracing::{info, warn};

use crate::{
    config::{Config, Mode},
    db::{Database, Job},
    error::ProxyError,
    openai::{BatchObject, OpenAiClient, copy_response_headers},
    request::{NormalizedRequest, UpstreamAuth, resolve_mode},
    sse,
};

struct AppState {
    config: Config,
    db: Database,
    upstream: OpenAiClient,
    pollers: Mutex<HashSet<String>>,
    notifiers: Mutex<HashMap<String, Arc<Notify>>>,
}

pub async fn build_router(config: Config) -> Result<Router, ProxyError> {
    let db = Database::connect(&config.database_url).await?;
    let upstream = OpenAiClient::new(&config.upstream_base_url)?;
    let max_body_bytes = config.max_body_bytes;
    let state = Arc::new(AppState {
        config,
        db,
        upstream,
        pollers: Mutex::new(HashSet::new()),
        notifiers: Mutex::new(HashMap::new()),
    });

    Ok(Router::new()
        .route("/healthz", get(health))
        .route("/responses", post(responses))
        .route("/v1/responses", post(responses))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state))
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn responses(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ProxyError> {
    match resolve_mode(&headers, state.config.mode)? {
        Mode::Direct => direct_response(&state, &headers, &body).await,
        Mode::Batch => batch_response(state, &headers, body).await,
    }
}

async fn direct_response(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
) -> Result<Response, ProxyError> {
    UpstreamAuth::from_headers(headers)?;
    let upstream = state.upstream.direct(headers, body).await?;
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let body = Body::from_stream(upstream.bytes_stream());
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_response_headers(&upstream_headers, response.headers_mut());
    response
        .headers_mut()
        .insert("x-kaiion-mode", HeaderValue::from_static("direct"));
    Ok(response)
}

async fn batch_response(
    state: Arc<AppState>,
    headers: &HeaderMap,
    body: Value,
) -> Result<Response, ProxyError> {
    let auth = UpstreamAuth::from_headers(headers)?;
    let auth_hash = auth.fingerprint();
    let request = NormalizedRequest::from_body(&body)?;
    if !request.stream {
        return Err(ProxyError::BadRequest(
            "batch mode currently requires stream=true".to_string(),
        ));
    }

    state
        .db
        .mark_other_requests_delivered(
            &auth_hash,
            request.session_key.as_deref(),
            &request.request_hash,
        )
        .await?;
    let job = state
        .db
        .get_or_create(
            &auth_hash,
            &request.request_hash,
            request.session_key.as_deref(),
            &request.model,
        )
        .await?;
    let notify = state.notifier(&job.id).await;
    state
        .clone()
        .start_poller(job.id.clone(), auth, request.batch_body.clone())
        .await;

    let job_id = job.id.clone();
    let model = job.model.clone();
    let db = state.db.clone();
    let heartbeat_period = state.config.in_progress_interval();
    let response_stream = stream! {
        let mut heartbeat = tokio::time::interval(heartbeat_period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let current = match db.get(&job_id).await {
                Ok(job) => job,
                Err(error) => {
                    yield Ok::<Bytes, Infallible>(sse::failed_event(&job_id, &model, &error.to_string()));
                    break;
                }
            };

            match current.status.as_str() {
                "completed" => {
                    let Some(result_json) = current.result_json else {
                        yield Ok(sse::failed_event(
                            &job_id,
                            &model,
                            "completed job has no stored response",
                        ));
                        break;
                    };
                    match serde_json::from_str::<Value>(&result_json)
                        .map_err(ProxyError::from)
                        .and_then(|response| sse::completed_events(&response))
                    {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
                            }
                            if let Err(error) = db.mark_delivered(&job_id).await {
                                warn!(%job_id, %error, "failed to mark response delivered");
                            }
                        }
                        Err(error) => {
                            yield Ok(sse::failed_event(&job_id, &model, &error.to_string()));
                        }
                    }
                    break;
                }
                "failed" | "expired" | "cancelled" => {
                    let message = current
                        .error_json
                        .as_deref()
                        .unwrap_or("batch request failed");
                    yield Ok(sse::failed_event(&job_id, &model, message));
                    break;
                }
                _ => {}
            }

            tokio::select! {
                _ = heartbeat.tick() => {
                    yield Ok(sse::in_progress_event(&job_id, &model));
                }
                _ = notify.notified() => {}
            }
        }
    };

    let mut response = Response::new(Body::from_stream(response_stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response
        .headers_mut()
        .insert("x-kaiion-mode", HeaderValue::from_static("batch"));
    response.headers_mut().insert(
        "x-kaiion-job-id",
        HeaderValue::from_str(&job.id)
            .map_err(|error| ProxyError::Internal(error.to_string()))?,
    );
    Ok(response)
}

impl AppState {
    async fn notifier(&self, job_id: &str) -> Arc<Notify> {
        let mut notifiers = self.notifiers.lock().await;
        notifiers
            .entry(job_id.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    async fn start_poller(
        self: Arc<Self>,
        job_id: String,
        auth: UpstreamAuth,
        request_body: Value,
    ) {
        {
            let mut pollers = self.pollers.lock().await;
            if !pollers.insert(job_id.clone()) {
                return;
            }
        }

        tokio::spawn(async move {
            info!(%job_id, "starting batch poller");
            loop {
                match self.drive_job(&job_id, &auth, &request_body).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) if error.retryable() => {
                        warn!(%job_id, %error, "transient batch error");
                    }
                    Err(error) => {
                        warn!(%job_id, %error, "batch job failed");
                        if let Err(store_error) = self
                            .db
                            .mark_failed(&job_id, "failed", &error.to_string())
                            .await
                        {
                            warn!(%job_id, %store_error, "failed to persist batch error");
                        }
                        self.notifier(&job_id).await.notify_waiters();
                        break;
                    }
                }
                tokio::time::sleep(self.config.poll_interval()).await;
            }
            self.pollers.lock().await.remove(&job_id);
            self.notifier(&job_id).await.notify_waiters();
            info!(%job_id, "batch poller stopped");
        });
    }

    async fn drive_job(
        &self,
        job_id: &str,
        auth: &UpstreamAuth,
        request_body: &Value,
    ) -> Result<bool, ProxyError> {
        let mut job = self.db.get(job_id).await?;
        if job.is_terminal() {
            return Ok(true);
        }

        if job.status == "queued" {
            let batch_line = serde_json::to_string(&json!({
                "custom_id": job.custom_id(),
                "method": "POST",
                "url": "/v1/responses",
                "body": request_body
            }))? + "\n";
            let file_id = self
                .upstream
                .upload_batch_file(auth, &job.id, batch_line)
                .await?;
            self.db.mark_file_uploaded(&job.id, &file_id).await?;
            self.notifier(&job.id).await.notify_waiters();
            job = self.db.get(job_id).await?;
        }

        if job.status == "file_uploaded" {
            let batch = match self
                .upstream
                .find_batch(auth, &job.id, job.attempt)
                .await?
            {
                Some(batch) => batch,
                None => {
                    let input_file_id = job.input_file_id.as_deref().ok_or_else(|| {
                        ProxyError::Internal("uploaded job has no input file ID".to_string())
                    })?;
                    self.upstream
                        .create_batch(auth, input_file_id, &job.id, job.attempt)
                        .await?
                }
            };
            self.db.mark_submitted(&job.id, &batch.id).await?;
            self.notifier(&job.id).await.notify_waiters();
            job = self.db.get(job_id).await?;
        }

        if job.status != "submitted" {
            return Ok(job.is_terminal());
        }

        let batch_id = job
            .batch_id
            .as_deref()
            .ok_or_else(|| ProxyError::Internal("submitted job has no batch ID".to_string()))?;
        let batch = self.upstream.get_batch(auth, batch_id).await?;
        self.apply_batch_status(&job, auth, &batch).await
    }

    async fn apply_batch_status(
        &self,
        job: &Job,
        auth: &UpstreamAuth,
        batch: &BatchObject,
    ) -> Result<bool, ProxyError> {
        match batch.status.as_str() {
            "completed" => {
                let response = self.read_batch_result(job, auth, batch).await?;
                self.db
                    .mark_completed(
                        &job.id,
                        batch.output_file_id.as_deref(),
                        &serde_json::to_string(&response)?,
                    )
                    .await?;
                self.notifier(&job.id).await.notify_waiters();
                Ok(true)
            }
            "failed" | "expired" | "cancelled" => {
                let error = if let Some(file_id) = &batch.error_file_id {
                    self.upstream.get_file_content(auth, file_id).await?
                } else {
                    serde_json::to_string(&json!({
                        "batch_id": batch.id,
                        "status": batch.status
                    }))?
                };
                self.db
                    .mark_failed(&job.id, &batch.status, &error)
                    .await?;
                self.notifier(&job.id).await.notify_waiters();
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn read_batch_result(
        &self,
        job: &Job,
        auth: &UpstreamAuth,
        batch: &BatchObject,
    ) -> Result<Value, ProxyError> {
        let file_id = batch.output_file_id.as_deref().ok_or_else(|| {
            ProxyError::Internal("completed batch has no output file ID".to_string())
        })?;
        let content = self.upstream.get_file_content(auth, file_id).await?;
        parse_batch_output(&content, &job.custom_id())
    }
}

fn parse_batch_output(content: &str, custom_id: &str) -> Result<Value, ProxyError> {
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)?;
        if value.get("custom_id").and_then(Value::as_str) != Some(custom_id) {
            continue;
        }
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            return Err(ProxyError::BatchResult(error.to_string()));
        }
        let response = value.get("response").ok_or_else(|| {
            ProxyError::Internal("batch output has no response".to_string())
        })?;
        let status = response
            .get("status_code")
            .and_then(Value::as_u64)
            .unwrap_or(500) as u16;
        if !(200..300).contains(&status) {
            return Err(ProxyError::BatchResult(
                response
                    .get("body")
                    .map(Value::to_string)
                    .unwrap_or_else(|| format!("HTTP {status}")),
            ));
        }
        return response.get("body").cloned().ok_or_else(|| {
            ProxyError::Internal("batch response has no body".to_string())
        });
    }
    Err(ProxyError::Internal(format!(
        "batch output does not contain custom_id {custom_id}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_matching_batch_output() {
        let content = concat!(
            "{\"custom_id\":\"other\",\"response\":null,\"error\":{\"message\":\"no\"}}\n",
            "{\"custom_id\":\"wanted\",\"response\":{\"status_code\":200,\"body\":{\"id\":\"resp_1\",\"output\":[]}},\"error\":null}\n"
        );
        let response = parse_batch_output(content, "wanted").unwrap();
        assert_eq!(response["id"], "resp_1");
    }
}
