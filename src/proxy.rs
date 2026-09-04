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
    jobs,
    openai::{OpenAiClient, copy_response_headers},
    request::{NormalizedRequest, UpstreamAuth, canonical_provider_url, resolve_mode},
    routing::{RouteDecision, RoutingPolicy},
    sse,
};

pub(crate) struct AppState {
    pub(crate) config: Config,
    pub(crate) db: Database,
    upstream: OpenAiClient,
    pub(crate) workers: WorkerRegistry,
    pub(crate) provider: String,
    policy: RoutingPolicy,
}

pub async fn build_router(config: Config) -> Result<Router, ProxyError> {
    let policy = RoutingPolicy::load(config.routing_policy.as_deref())?;
    let provider = canonical_provider_url(&config.upstream_base_url)?;
    let db = Database::connect(&config.database_url).await?;
    let upstream = OpenAiClient::new(&config.upstream_base_url)?;
    let workers = WorkerRegistry::new(db.clone(), upstream.clone(), config.poll_interval());
    let max_body_bytes = config.max_body_bytes;
    let state = Arc::new(AppState {
        config,
        db,
        upstream,
        workers,
        policy,
        provider,
    });
    if state.config.resume_from_env {
        jobs::resume_from_env(&state).await?;
    }
    Ok(Router::new()
        .route("/healthz", get(health))
        .route("/responses", post(responses))
        .route("/v1/responses", post(responses))
        .route("/v1/kaiion/jobs", post(jobs::submit).get(jobs::list))
        .route("/v1/kaiion/jobs/{id}", get(jobs::get_job))
        .route("/v1/kaiion/jobs/{id}/resume", post(jobs::resume))
        .route("/v1/kaiion/route", post(explain_route))
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
    let decision = route(&state, &headers, &body).await?;
    let mut response = match decision.mode {
        Mode::Direct => direct_response(&state, &headers, &body).await?,
        Mode::Batch => batch_response(state, &headers, body).await?,
        Mode::Auto => unreachable!("routing resolves auto to an execution mode"),
    };
    response.headers_mut().insert(
        "x-kaiion-route-reason",
        HeaderValue::from_static(decision.reason),
    );
    Ok(response)
}

async fn route(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
) -> Result<RouteDecision, ProxyError> {
    let mode = resolve_mode(headers, state.config.mode)?;
    if mode != Mode::Auto {
        return Ok(RouteDecision::new(mode, "explicit_mode"));
    }
    let auth = UpstreamAuth::from_headers(headers)?;
    let request = NormalizedRequest::from_headers(body, &state.provider, headers)?;
    state
        .db
        .check_idempotency(&auth.fingerprint(), &request)
        .await?;
    if state
        .db
        .find(&auth.fingerprint(), &request.request_hash)
        .await?
        .is_some()
    {
        return Ok(RouteDecision::new(Mode::Batch, "existing_batch_job"));
    }
    Ok(state.policy.decide(body))
}

async fn explain_route(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<RouteDecision>, ProxyError> {
    UpstreamAuth::from_headers(&headers)?;
    Ok(Json(route(&state, &headers, &body).await?))
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
    let request = NormalizedRequest::from_headers(&body, &state.config.upstream_base_url, headers)?;
    let job = state
        .db
        .enqueue(&auth.fingerprint(), &state.provider, &request)
        .await?;
    let mut states = state
        .workers
        .subscribe(job.clone(), auth, request.batch_body)
        .await;
    if body.get("stream").and_then(Value::as_bool) != Some(true) {
        loop {
            if let JobState::Terminal(outcome) = states.borrow_and_update().clone() {
                let mut response = Json(jobs::response_value(&outcome, &job.id)).into_response();
                set_batch_headers(&mut response, &job.id.0)?;
                return Ok(response);
            }
            states.changed().await.map_err(|_| {
                ProxyError::Internal("batch worker stopped before reaching a terminal state".into())
            })?;
        }
    }
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
    set_batch_headers(&mut response, &job.id.0)?;
    Ok(response)
}

fn set_batch_headers(response: &mut Response, job_id: &str) -> Result<(), ProxyError> {
    response
        .headers_mut()
        .insert("x-kaiion-mode", HeaderValue::from_static("batch"));
    response.headers_mut().insert(
        "x-kaiion-job-id",
        HeaderValue::from_str(job_id).map_err(|error| ProxyError::Internal(error.to_string()))?,
    );
    Ok(())
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
