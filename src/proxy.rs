use std::{convert::Infallible, sync::Arc};

use async_stream::stream;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use serde_json::{Value, json};

use crate::{
    config::{Config, Mode},
    db::Database,
    domain::{JobState, StoredOutcome},
    driver::WorkerRegistry,
    error::ProxyError,
    openai::{OpenAiClient, copy_response_headers},
    request::{NormalizedRequest, UpstreamAuth, resolve_mode},
    sse,
};

struct AppState {
    config: Config,
    db: Database,
    upstream: OpenAiClient,
    workers: WorkerRegistry,
}

pub async fn build_router(config: Config) -> Result<Router, ProxyError> {
    let db = Database::connect(&config.database_url).await?;
    let upstream = OpenAiClient::new(&config.upstream_base_url)?;
    let workers = WorkerRegistry::new(db.clone(), upstream.clone(), config.poll_interval());
    let max_body_bytes = config.max_body_bytes;
    let state = Arc::new(AppState {
        config,
        db,
        upstream,
        workers,
    });
    Ok(Router::new()
        .route("/healthz", get(health))
        .route("/responses", post(responses))
        .route("/v1/responses", post(responses))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state))
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "active_batch_workers": state.workers.active_count().await
    }))
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
    let request = NormalizedRequest::from_body(&body, &state.config.upstream_base_url)?;
    let job = state
        .db
        .get_or_create(&auth.fingerprint(), &request.request_hash, &request.model)
        .await?;
    let mut states = state
        .workers
        .subscribe(job.clone(), auth, request.batch_body)
        .await;
    let response_id = format!("resp_kaiion_{}", job.id);
    let heartbeat_period = state.config.in_progress_interval();
    let model = job.model.clone();
    let response_stream = stream! {
        let mut sequence = 0_u64;
        yield Ok::<Bytes, Infallible>(sse::created_event(&response_id, &model, sequence));
        sequence += 1;
        yield Ok(sse::in_progress_event(&response_id, &model, sequence));
        sequence += 1;

        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + heartbeat_period,
            heartbeat_period,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let current = states.borrow_and_update().clone();
            match current {
                JobState::Terminal(StoredOutcome::Completed(response)) => {
                    match sse::completion_events(&response, &response_id, sequence) {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
                            }
                        }
                        Err(error) => yield Ok(sse::failed_event(
                            &response_id,
                            &model,
                            sequence,
                            &error.to_string(),
                        )),
                    }
                    break;
                }
                JobState::Terminal(outcome) => {
                    let value = terminal_value(outcome);
                    yield Ok(sse::terminal_error_event(
                        &response_id,
                        &model,
                        sequence,
                        &value.to_string(),
                    ));
                    break;
                }
                _ => {}
            }
            tokio::select! {
                _ = heartbeat.tick() => {
                    yield Ok(sse::in_progress_event(&response_id, &model, sequence));
                    sequence += 1;
                }
                changed = states.changed() => {
                    if changed.is_err() {
                        yield Ok(sse::failed_event(
                            &response_id,
                            &model,
                            sequence,
                            "batch worker stopped before reaching a terminal state",
                        ));
                        break;
                    }
                }
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
        HeaderValue::from_str(&job.id.0)
            .map_err(|error| ProxyError::Internal(error.to_string()))?,
    );
    Ok(response)
}

fn terminal_value(outcome: StoredOutcome) -> Value {
    match outcome {
        StoredOutcome::Completed(value)
        | StoredOutcome::Failed(value)
        | StoredOutcome::Incomplete(value)
        | StoredOutcome::Expired(value)
        | StoredOutcome::Cancelled(value) => value,
    }
}
