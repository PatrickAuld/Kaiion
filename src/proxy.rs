use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
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
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::{
    config::{Config, Mode},
    db::Database,
    domain::{JobState, StoredOutcome},
    driver::WorkerRegistry,
    error::ProxyError,
    jobs,
    openai::{OpenAiClient, copy_response_headers, retry_after},
    request::{
        IDEMPOTENCY_HEADER, NormalizedRequest, UpstreamAuth, batch_supported,
        canonical_provider_url, estimate_tokens, is_tool_output, is_user_prompt, model_alias,
        resolve_mode, response_usage, scheduling_session_id, upstream_body,
    },
    routing::{RouteDecision, RoutingPolicy},
    scheduling::{InteractionKind, ScheduleOutcome, SchedulePermit, ScheduleRequest, Scheduler},
    scheduling_policy::SchedulingPolicy,
    sse,
};

pub(crate) struct AppState {
    pub(crate) config: Config,
    pub(crate) db: Database,
    upstream: OpenAiClient,
    pub(crate) workers: WorkerRegistry,
    pub(crate) provider: String,
    policy: RoutingPolicy,
    scheduler: Option<Scheduler>,
    scheduling_policy: Option<SchedulingPolicy>,
}

pub async fn build_router(config: Config) -> Result<Router, ProxyError> {
    let policy = RoutingPolicy::load(config.routing_policy.as_deref())?;
    let scheduling_policy = config
        .scheduling_policy
        .as_deref()
        .map(|path| SchedulingPolicy::load(Some(path)))
        .transpose()?;
    let scheduler = scheduling_policy
        .as_ref()
        .filter(|policy| policy.enabled)
        .map(|policy| {
            Scheduler::new(policy.clone()).map_err(|error| {
                ProxyError::BadRequest(format!("invalid scheduling policy: {error}"))
            })
        })
        .transpose()?;
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
        scheduler,
        scheduling_policy,
        provider,
    });
    if state.config.resume_from_env {
        jobs::resume_from_env(&state).await?;
    }
    Ok(Router::new()
        .route("/healthz", get(health))
        .route("/models", get(models))
        .route("/v1/models", get(models))
        .route("/responses", post(responses))
        .route("/v1/responses", post(responses))
        .route("/v1/kaiion/jobs", post(jobs::submit).get(jobs::list))
        .route("/v1/kaiion/jobs/{id}", get(jobs::get_job))
        .route("/v1/kaiion/jobs/{id}/resume", post(jobs::resume))
        .route("/v1/kaiion/route", post(explain_route))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state))
}

async fn models(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ProxyError> {
    UpstreamAuth::from_headers(&headers)?;
    let upstream = state.upstream.list_models(&headers).await?;
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let bytes = upstream.bytes().await?;
    let response_body = if status.is_success() {
        serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|mut value| {
                let data = value.get_mut("data")?.as_array_mut()?;
                let aliases: Vec<Value> = data
                    .iter()
                    .filter_map(|model| {
                        let id = model.get("id")?.as_str()?;
                        if id.starts_with("async-") {
                            return None;
                        }
                        let mut alias = model.clone();
                        alias
                            .as_object_mut()?
                            .insert("id".into(), Value::String(format!("async-{id}")));
                        Some(alias)
                    })
                    .collect();
                data.extend(aliases);
                serde_json::to_vec(&value).ok()
            })
            .unwrap_or_else(|| bytes.to_vec())
    } else {
        bytes.to_vec()
    };
    let mut response = Response::new(Body::from(response_body));
    *response.status_mut() = status;
    copy_response_headers(&upstream_headers, response.headers_mut());
    Ok(response)
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
    let requested_mode = resolve_mode(&headers, state.config.mode)?;
    let mut decision = route(&state, &headers, &body).await?;
    let mut response = match decision.mode {
        Mode::Direct if state.scheduler.is_some() => {
            match scheduled_direct_response(&state, &headers, &body, requested_mode).await? {
                ScheduledDirect::Live(response) => response,
                ScheduledDirect::Batch(reason) => {
                    decision.mode = Mode::Batch;
                    decision.reason = reason;
                    batch_response(state, &headers, body).await?
                }
                ScheduledDirect::Unavailable(response) => response,
            }
        }
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
    let batch_request = if batch_supported(body, headers)? {
        Some(NormalizedRequest::from_headers(
            body,
            &state.provider,
            headers,
        )?)
    } else {
        None
    };
    if let Some(request) = &batch_request {
        state
            .db
            .check_idempotency(&auth.fingerprint(), request)
            .await?;
        if state
            .db
            .find(&auth.fingerprint(), &request.request_hash)
            .await?
            .is_some()
        {
            return Ok(RouteDecision::new(Mode::Batch, "existing_batch_job"));
        }
    }
    let provider_body = upstream_body(body)?;
    let decision = state.policy.decide(&provider_body);
    if decision.mode == Mode::Batch && batch_request.is_none() {
        return Ok(RouteDecision::new(Mode::Direct, "batch_not_supported"));
    }
    Ok(decision)
}

fn make_schedule_request(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
    requested_mode: Mode,
    retry_attempt: u32,
    upstream_retry_after: Option<Duration>,
) -> Result<ScheduleRequest, ProxyError> {
    let auth = UpstreamAuth::from_headers(headers)?;
    let requested_model = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| ProxyError::BadRequest("missing model".into()))?;
    let (model, async_alias) = model_alias(requested_model);
    if model.is_empty() {
        return Err(ProxyError::BadRequest(
            "async- model alias must include an upstream model name".into(),
        ));
    }
    let (input_tokens, output_tokens) = estimate_tokens(body);
    let interaction = if is_tool_output(body) {
        InteractionKind::ToolOutput
    } else if is_user_prompt(body) {
        InteractionKind::UserPrompt
    } else {
        InteractionKind::Unknown
    };
    let allow_batch_fallback = requested_mode == Mode::Auto && batch_supported(body, headers)?;
    Ok(ScheduleRequest {
        provider: state.provider.clone(),
        auth_scope: auth.fingerprint(),
        model: model.to_string(),
        session_id: scheduling_session_id(body, headers)?,
        input_tokens,
        output_tokens,
        interaction,
        async_alias,
        cache_read_hit: None,
        retry_attempt,
        upstream_retry_after,
        allow_batch_fallback,
        body: Some(upstream_body(body)?),
    })
}

async fn explain_route(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ProxyError> {
    UpstreamAuth::from_headers(&headers)?;
    let requested_mode = resolve_mode(&headers, state.config.mode)?;
    let mut decision = route(&state, &headers, &body).await?;
    let async_alias = body
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| model_alias(model).1);
    let recommendation = if decision.mode == Mode::Direct {
        if let Some(scheduler) = &state.scheduler {
            let request = make_schedule_request(&state, &headers, &body, requested_mode, 0, None)?;
            let recommendation = scheduler.preview(&request).await;
            if let Some(reason) = recommendation.filter(|_| request.allow_batch_fallback) {
                decision.mode = Mode::Batch;
                decision.reason = reason;
            }
            recommendation
        } else {
            None
        }
    } else {
        None
    };
    let mut value = serde_json::to_value(decision)?;
    value["scheduling"] = json!({
        "enabled": state.scheduler.is_some(),
        "async_alias": async_alias,
        "recommendation": recommendation,
    });
    Ok(Json(value))
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

enum ScheduledDirect {
    Live(Response),
    Batch(&'static str),
    Unavailable(Response),
}

async fn scheduled_direct_response(
    state: &AppState,
    headers: &HeaderMap,
    body: &Value,
    requested_mode: Mode,
) -> Result<ScheduledDirect, ProxyError> {
    let scheduler = state
        .scheduler
        .as_ref()
        .expect("scheduled path requires an enabled scheduler");
    let policy = state
        .scheduling_policy
        .as_ref()
        .expect("enabled scheduler has a policy");
    let first_request = make_schedule_request(state, headers, body, requested_mode, 0, None)?;
    let allow_batch_fallback = first_request.allow_batch_fallback;
    let max_wait = policy.max_wait(first_request.async_alias);
    if allow_batch_fallback && let Some(reason) = scheduler.preview(&first_request).await {
        return Ok(ScheduledDirect::Batch(reason));
    }
    let deadline = Instant::now() + max_wait;
    let mut attempt = 0;
    let mut upstream_retry_after = None;
    let mut last_request = first_request;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            if allow_batch_fallback && let Some(reason) = scheduler.preview(&last_request).await {
                return Ok(ScheduledDirect::Batch(reason));
            }
            return Ok(ScheduledDirect::Unavailable(scheduling_unavailable(
                "max_wait_exceeded",
                None,
            )));
        }
        let schedule_request = make_schedule_request(
            state,
            headers,
            body,
            requested_mode,
            attempt,
            upstream_retry_after,
        )?;
        last_request = schedule_request.clone();
        let outcome = match tokio::time::timeout(
            remaining,
            scheduler.acquire(schedule_request.clone()),
        )
        .await
        {
            Err(_) => {
                if allow_batch_fallback
                    && let Some(reason) = scheduler.preview(&schedule_request).await
                {
                    return Ok(ScheduledDirect::Batch(reason));
                }
                return Ok(ScheduledDirect::Unavailable(scheduling_unavailable(
                    "max_wait_exceeded",
                    None,
                )));
            }
            Ok(Err(error)) => return Err(ProxyError::Internal(error.to_string())),
            Ok(Ok(outcome)) => outcome,
        };

        let permit = match outcome {
            ScheduleOutcome::Dispatch(permit) => permit,
            ScheduleOutcome::RecommendBatch { reason, .. } if allow_batch_fallback => {
                return Ok(ScheduledDirect::Batch(reason));
            }
            ScheduleOutcome::RecommendBatch { retry_after, .. } => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                return Ok(ScheduledDirect::Unavailable(scheduling_unavailable(
                    "batch_fallback_not_allowed",
                    retry_after.filter(|delay| *delay <= remaining),
                )));
            }
            ScheduleOutcome::RetryExhausted {
                reason,
                retry_after,
            } => {
                if allow_batch_fallback
                    && let Some(batch_reason) = scheduler.preview(&schedule_request).await
                {
                    return Ok(ScheduledDirect::Batch(batch_reason));
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                return Ok(ScheduledDirect::Unavailable(scheduling_unavailable(
                    reason,
                    retry_after.filter(|delay| *delay <= remaining),
                )));
            }
        };

        let upstream = match state.upstream.direct(headers, body).await {
            Ok(response) => response,
            Err(ProxyError::Transport(error))
                if error.is_connect() && attempt < policy.max_retries =>
            {
                scheduler.abort(permit).await;
                attempt += 1;
                upstream_retry_after = None;
                continue;
            }
            Err(error) => {
                if matches!(&error, ProxyError::Transport(transport) if transport.is_connect()) {
                    scheduler.abort(permit).await;
                } else {
                    scheduler.complete(permit, None, None, None).await;
                }
                return Err(error);
            }
        };

        let status = upstream.status();
        let safe_server_retry =
            status.is_server_error() && headers.contains_key(IDEMPOTENCY_HEADER);
        if (status == StatusCode::TOO_MANY_REQUESTS || safe_server_retry)
            && attempt < policy.max_retries
        {
            let retry_delay = retry_after(upstream.headers());
            let remaining = deadline.saturating_duration_since(Instant::now());
            if retry_delay.is_some_and(|delay| delay > remaining) {
                if status == StatusCode::TOO_MANY_REQUESTS
                    && allow_batch_fallback
                    && scheduler.preview(&schedule_request).await.is_some()
                {
                    scheduler.complete(permit, Some(0), Some(0), None).await;
                    return Ok(ScheduledDirect::Batch(
                        "provider_retry_after_batch_fallback",
                    ));
                }
                return Ok(ScheduledDirect::Live(
                    scheduled_upstream_response(upstream, scheduler.clone(), permit).await?,
                ));
            }
            let (actual_input, actual_output) = if status == StatusCode::TOO_MANY_REQUESTS {
                (Some(0), Some(0))
            } else {
                (None, None)
            };
            scheduler
                .complete(permit, actual_input, actual_output, None)
                .await;
            drop(upstream);
            attempt += 1;
            upstream_retry_after = retry_delay;
            continue;
        }

        return Ok(ScheduledDirect::Live(
            scheduled_upstream_response(upstream, scheduler.clone(), permit).await?,
        ));
    }
}

fn scheduling_unavailable(reason: &str, retry_after: Option<Duration>) -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": {
                "message": format!("scheduled live request could not be dispatched: {reason}"),
                "type": "kaiion_error",
                "code": "scheduling_unavailable"
            }
        })),
    )
        .into_response();
    if let Some(delay) = retry_after.filter(|delay| !delay.is_zero())
        && let Ok(value) = HeaderValue::from_str(&delay.as_secs().max(1).to_string())
    {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
        .headers_mut()
        .insert("x-kaiion-mode", HeaderValue::from_static("direct"));
    response
}

async fn scheduled_upstream_response(
    upstream: reqwest::Response,
    scheduler: Scheduler,
    permit: SchedulePermit,
) -> Result<Response, ProxyError> {
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let is_event_stream = upstream_headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().starts_with("text/event-stream"));
    let mut response = if is_event_stream {
        let mut stream = upstream.bytes_stream();
        let stream_body = stream! {
            let mut usage = SseUsageTracker::default();
            let mut permit = Some(permit);
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        usage.push(&bytes);
                        yield Ok::<Bytes, reqwest::Error>(bytes);
                    }
                    Err(error) => {
                        if let Some(permit) = permit.take() {
                            let (input, output, cache_hit) = usage.usage.unwrap_or((None, None, None));
                            scheduler.complete(permit, input, output, cache_hit).await;
                        }
                        yield Err(error);
                        return;
                    }
                }
            }
            usage.finish();
            if let Some(permit) = permit.take() {
                let (input, output, cache_hit) = usage.usage.unwrap_or((None, None, None));
                scheduler.complete(permit, input, output, cache_hit).await;
            }
        };
        let mut response = Response::new(Body::from_stream(stream_body));
        *response.status_mut() = status;
        copy_response_headers(&upstream_headers, response.headers_mut());
        response
    } else {
        let bytes = match upstream.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                scheduler.complete(permit, None, None, None).await;
                return Err(error.into());
            }
        };
        let usage = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .map(|value| response_usage(&value))
            .unwrap_or((None, None, None));
        scheduler.complete(permit, usage.0, usage.1, usage.2).await;
        let mut response = Response::new(Body::from(bytes));
        *response.status_mut() = status;
        copy_response_headers(&upstream_headers, response.headers_mut());
        response
    };
    response
        .headers_mut()
        .insert("x-kaiion-mode", HeaderValue::from_static("direct"));
    Ok(response)
}

#[derive(Default)]
struct SseUsageTracker {
    line: Vec<u8>,
    data: Vec<u8>,
    usage: Option<(Option<u64>, Option<u64>, Option<bool>)>,
    oversized: bool,
}

impl SseUsageTracker {
    const MAX_EVENT_BYTES: usize = 2 * 1024 * 1024;

    fn push(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if *byte == b'\n' {
                self.process_line();
                self.line.clear();
            } else if self.line.len() < Self::MAX_EVENT_BYTES {
                self.line.push(*byte);
            }
        }
    }

    fn finish(&mut self) {
        if !self.line.is_empty() {
            self.process_line();
            self.line.clear();
        }
        self.process_event();
    }

    fn process_line(&mut self) {
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.line.is_empty() {
            self.process_event();
        } else if let Some(value) = self.line.strip_prefix(b"data:")
            && !self.oversized
        {
            let value = value.strip_prefix(b" ").unwrap_or(value);
            if self.data.len().saturating_add(value.len()) > Self::MAX_EVENT_BYTES {
                self.data.clear();
                self.oversized = true;
            } else {
                if !self.data.is_empty() {
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(value);
            }
        }
    }

    fn process_event(&mut self) {
        if !self.oversized
            && !self.data.is_empty()
            && let Ok(value) = serde_json::from_slice::<Value>(&self.data)
        {
            let usage = response_usage(&value);
            if usage.0.is_some() || usage.1.is_some() || usage.2.is_some() {
                self.usage = Some(usage);
            }
        }
        self.data.clear();
        self.oversized = false;
    }
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
