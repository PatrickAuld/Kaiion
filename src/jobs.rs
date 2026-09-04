use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    domain::{Job, JobId, JobState, StoredOutcome},
    error::ProxyError,
    proxy::AppState,
    request::{NormalizedRequest, UpstreamAuth},
};

#[derive(Default, Deserialize)]
pub(crate) struct JobQuery {
    #[serde(default)]
    after: String,
}

pub(crate) async fn list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<JobQuery>,
) -> Result<Json<Value>, ProxyError> {
    let auth = UpstreamAuth::from_headers(&headers)?;
    let jobs = state
        .db
        .list_owned(&auth.fingerprint(), &state.provider, &query.after, 100)
        .await?;
    let next = if jobs.len() == 100 {
        jobs.last().map(|job| &job.id)
    } else {
        None
    };
    Ok(Json(json!({"data": jobs, "next_after": next})))
}

pub(crate) async fn resume_from_env(state: &AppState) -> Result<(), ProxyError> {
    let key = std::env::var("OPENAI_API_KEY")
        .map_err(|_| ProxyError::BadRequest("--resume-from-env requires OPENAI_API_KEY".into()))?;
    if key.trim().is_empty() {
        return Err(ProxyError::BadRequest(
            "OPENAI_API_KEY cannot be empty".into(),
        ));
    }
    let auth = UpstreamAuth {
        authorization: format!("Bearer {key}"),
        organization: std::env::var("OPENAI_ORG_ID").ok(),
        project: std::env::var("OPENAI_PROJECT_ID").ok(),
    };
    let mut after = String::new();
    loop {
        let jobs = state
            .db
            .list_owned(&auth.fingerprint(), &state.provider, &after, 100)
            .await?;
        if jobs.is_empty() {
            return Ok(());
        }
        for job in jobs {
            after = job.id.clone();
            if !job.terminal {
                let (job, body) = state
                    .db
                    .owned_request(&JobId(job.id), &auth.fingerprint(), &state.provider)
                    .await?;
                state.workers.subscribe(job, auth.clone(), body).await;
            }
        }
    }
}

pub(crate) async fn submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ProxyError> {
    let auth = UpstreamAuth::from_headers(&headers)?;
    let request =
        NormalizedRequest::from_headers(&body, &state.config.upstream_base_url, &headers)?;
    let job = state
        .db
        .enqueue(&auth.fingerprint(), &state.provider, &request)
        .await?;
    state
        .workers
        .subscribe(job.clone(), auth, request.batch_body)
        .await;
    Ok((
        StatusCode::ACCEPTED,
        [
            ("location", format!("/v1/kaiion/jobs/{}", job.id)),
            (
                "retry-after",
                state.config.poll_interval_seconds.max(1).to_string(),
            ),
        ],
        Json(job_view(&job)),
    )
        .into_response())
}

pub(crate) async fn get_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ProxyError> {
    let auth = UpstreamAuth::from_headers(&headers)?;
    let job = state
        .db
        .owned_job(&JobId(id), &auth.fingerprint(), &state.provider)
        .await?;
    Ok(Json(job_view(&job)).into_response())
}

pub(crate) async fn resume(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ProxyError> {
    let auth = UpstreamAuth::from_headers(&headers)?;
    let (job, body) = state
        .db
        .owned_request(&JobId(id), &auth.fingerprint(), &state.provider)
        .await?;
    state.workers.subscribe(job.clone(), auth, body).await;
    Ok((StatusCode::ACCEPTED, Json(job_view(&job))).into_response())
}

pub fn job_view(job: &Job) -> Value {
    let (status, result) = match &job.state {
        JobState::Queued => ("queued", None),
        JobState::Uploaded { .. } => ("uploaded", None),
        JobState::Submitting { .. } => ("submitting", None),
        JobState::SubmissionUncertain { .. } => ("submission_uncertain", None),
        JobState::Submitted { .. } => ("submitted", None),
        JobState::Terminal(outcome) => (
            outcome_status(outcome),
            Some(response_value(outcome, &job.id)),
        ),
    };
    json!({"id": job.id.0, "object": "kaiion.job", "model": job.model, "status": status, "terminal": job.state.is_terminal(), "response": result})
}

pub fn response_value(outcome: &StoredOutcome, id: &JobId) -> Value {
    let mut value = match outcome {
        StoredOutcome::Completed(value)
        | StoredOutcome::Failed(value)
        | StoredOutcome::Incomplete(value)
        | StoredOutcome::Expired(value)
        | StoredOutcome::Cancelled(value) => value.clone(),
    };
    if let Some(object) = value.as_object_mut() {
        object.insert("id".into(), json!(format!("resp_kaiion_{id}")));
    }
    value
}

fn outcome_status(outcome: &StoredOutcome) -> &'static str {
    match outcome {
        StoredOutcome::Completed(_) => "completed",
        StoredOutcome::Failed(_) => "failed",
        StoredOutcome::Incomplete(_) => "incomplete",
        StoredOutcome::Expired(_) => "expired",
        StoredOutcome::Cancelled(_) => "cancelled",
    }
}
