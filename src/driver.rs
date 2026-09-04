use std::{collections::HashMap, sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};
use tracing::{info, warn};

use crate::{
    db::{Database, PersistenceError},
    domain::{BatchId, Job, JobId, JobState, StoredOutcome},
    openai::{BatchObject, OpenAiClient, ProviderError},
    request::UpstreamAuth,
};

#[derive(Clone)]
pub struct WorkerRegistry {
    workers: Arc<Mutex<HashMap<JobId, watch::Sender<JobState>>>>,
    driver: Arc<JobDriver>,
}

struct JobDriver {
    db: Database,
    upstream: OpenAiClient,
    poll_interval: Duration,
}

impl WorkerRegistry {
    pub fn new(db: Database, upstream: OpenAiClient, poll_interval: Duration) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            driver: Arc::new(JobDriver {
                db,
                upstream,
                poll_interval,
            }),
        }
    }

    pub async fn subscribe(
        &self,
        job: Job,
        auth: UpstreamAuth,
        request_body: Value,
    ) -> watch::Receiver<JobState> {
        if job.state.is_terminal() {
            return watch::channel(job.state).1;
        }
        let mut workers = self.workers.lock().await;
        if let Some(sender) = workers.get(&job.id) {
            return sender.subscribe();
        }
        let (sender, receiver) = watch::channel(job.state.clone());
        workers.insert(job.id.clone(), sender.clone());
        drop(workers);

        let registry = self.clone();
        tokio::spawn(async move {
            let job_id = job.id.clone();
            registry.driver.run(job, auth, request_body, sender).await;
            registry.workers.lock().await.remove(&job_id);
            info!(%job_id, "batch worker stopped");
        });
        receiver
    }

    pub async fn active_count(&self) -> usize {
        self.workers.lock().await.len()
    }
}

impl JobDriver {
    async fn run(
        &self,
        mut job: Job,
        auth: UpstreamAuth,
        request_body: Value,
        sender: watch::Sender<JobState>,
    ) {
        info!(job_id = %job.id, "starting batch worker");
        loop {
            let result = self.advance(&job, &auth, &request_body).await;
            match result {
                Ok(next) => {
                    let changed = next != job.state;
                    job.state = next;
                    if changed {
                        sender.send_replace(job.state.clone());
                    }
                    if job.state.is_terminal() {
                        return;
                    }
                    if changed {
                        continue;
                    }
                }
                Err(DriverError::Conflict) => {
                    if let Some(state) = self.driver_state(&job.id).await {
                        job.state = state;
                        sender.send_replace(job.state.clone());
                        if job.state.is_terminal() {
                            return;
                        }
                        continue;
                    }
                }
                Err(DriverError::Retryable(error)) => {
                    warn!(job_id = %job.id, %error, "retryable batch operation failed");
                }
                Err(DriverError::Permanent(error)) => {
                    warn!(job_id = %job.id, %error, "batch job failed permanently");
                    match self
                        .db
                        .store_outcome(
                            &job.id,
                            &job.state,
                            StoredOutcome::Failed(failed_response(&job.model, &error)),
                        )
                        .await
                    {
                        Ok(state) => {
                            job.state = state;
                            sender.send_replace(job.state.clone());
                            return;
                        }
                        Err(error) if error.is_conflict() => {
                            if let Some(state) = self.driver_state(&job.id).await {
                                job.state = state;
                                sender.send_replace(job.state.clone());
                                if job.state.is_terminal() {
                                    return;
                                }
                            }
                            continue;
                        }
                        Err(error) => {
                            warn!(job_id = %job.id, %error, "failed to store terminal outcome");
                        }
                    }
                }
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    async fn driver_state(&self, job_id: &JobId) -> Option<JobState> {
        match self.db.get(job_id).await {
            Ok(job) => Some(job.state),
            Err(error) => {
                warn!(%job_id, %error, "failed to reconcile job state");
                None
            }
        }
    }

    async fn advance(
        &self,
        job: &Job,
        auth: &UpstreamAuth,
        request_body: &Value,
    ) -> Result<JobState, DriverError> {
        match &job.state {
            JobState::Queued => {
                let batch_line = serde_json::to_string(&json!({
                    "custom_id": job.custom_id(),
                    "method": "POST",
                    "url": "/v1/responses",
                    "body": request_body
                }))
                .map_err(|error| DriverError::Permanent(error.to_string()))?
                    + "\n";
                let file_id = self
                    .upstream
                    .upload_batch_file(auth, &job.id, batch_line)
                    .await
                    .map_err(classify_provider)?;
                self.db
                    .mark_uploaded(&job.id, file_id)
                    .await
                    .map_err(classify_persistence)
            }
            JobState::Uploaded { input_file_id } => {
                let submitting = self
                    .db
                    .begin_submission(&job.id, input_file_id)
                    .await
                    .map_err(classify_persistence)?;
                match self
                    .upstream
                    .create_batch(auth, input_file_id, &job.id)
                    .await
                {
                    Ok(batch) => self
                        .db
                        .mark_submitted(&job.id, &submitting, BatchId(batch.id))
                        .await
                        .map_err(classify_persistence),
                    Err(error) if error.retryable() => self
                        .db
                        .mark_submission_uncertain(&job.id)
                        .await
                        .map_err(classify_persistence),
                    Err(error) => Err(DriverError::Permanent(error.to_string())),
                }
            }
            JobState::Submitting { .. } => self
                .db
                .mark_submission_uncertain(&job.id)
                .await
                .map_err(classify_persistence),
            JobState::SubmissionUncertain { .. } => {
                match self.upstream.find_batch(auth, &job.id).await {
                    Ok(Some(batch)) => self
                        .db
                        .mark_submitted(&job.id, &job.state, BatchId(batch.id))
                        .await
                        .map_err(classify_persistence),
                    Ok(None) => Ok(job.state.clone()),
                    Err(error) => Err(classify_provider(error)),
                }
            }
            JobState::Submitted { batch_id } => {
                let batch = self
                    .upstream
                    .get_batch(auth, batch_id)
                    .await
                    .map_err(classify_provider)?;
                self.apply_batch_status(job, auth, &batch).await
            }
            JobState::Terminal(_) => Ok(job.state.clone()),
        }
    }

    async fn apply_batch_status(
        &self,
        job: &Job,
        auth: &UpstreamAuth,
        batch: &BatchObject,
    ) -> Result<JobState, DriverError> {
        match batch.status.as_str() {
            "completed" => {
                if batch.output_file_id.is_none()
                    && let Some(file_id) = &batch.error_file_id
                {
                    let content = self
                        .upstream
                        .get_file_content(auth, file_id)
                        .await
                        .map_err(classify_provider)?;
                    let outcome = StoredOutcome::Failed(normalize_batch_error(
                        &content,
                        &job.custom_id(),
                        &job.model,
                    ));
                    return self
                        .db
                        .store_outcome(&job.id, &job.state, outcome)
                        .await
                        .map_err(classify_persistence);
                }
                let file_id = batch.output_file_id.as_deref().ok_or_else(|| {
                    DriverError::Permanent(
                        "completed batch has neither output_file_id nor error_file_id".to_string(),
                    )
                })?;
                let content = self
                    .upstream
                    .get_file_content(auth, file_id)
                    .await
                    .map_err(classify_provider)?;
                let response = parse_batch_output(&content, &job.custom_id(), &job.model)?;
                let outcome = match response.get("status").and_then(Value::as_str) {
                    Some("completed") => StoredOutcome::Completed(response),
                    Some("failed") => StoredOutcome::Failed(response),
                    Some("incomplete") => StoredOutcome::Incomplete(response),
                    Some(status) => {
                        return Err(DriverError::Permanent(format!(
                            "batch response has unsupported terminal status {status:?}"
                        )));
                    }
                    None => {
                        return Err(DriverError::Permanent(
                            "batch response is missing status".to_string(),
                        ));
                    }
                };
                self.db
                    .store_outcome(&job.id, &job.state, outcome)
                    .await
                    .map_err(classify_persistence)
            }
            "failed" | "expired" | "cancelled" => {
                let value = if let Some(file_id) = &batch.error_file_id {
                    let content = self
                        .upstream
                        .get_file_content(auth, file_id)
                        .await
                        .map_err(classify_provider)?;
                    normalize_batch_error(&content, &job.custom_id(), &job.model)
                } else {
                    failed_response(
                        &job.model,
                        &format!("batch {} ended with status {}", batch.id, batch.status),
                    )
                };
                let outcome = match batch.status.as_str() {
                    "expired" => StoredOutcome::Expired(value),
                    "cancelled" => StoredOutcome::Cancelled(value),
                    _ => StoredOutcome::Failed(value),
                };
                self.db
                    .store_outcome(&job.id, &job.state, outcome)
                    .await
                    .map_err(classify_persistence)
            }
            _ => Ok(job.state.clone()),
        }
    }
}

#[derive(Debug)]
enum DriverError {
    Conflict,
    Retryable(String),
    Permanent(String),
}

fn classify_provider(error: ProviderError) -> DriverError {
    if error.retryable() {
        DriverError::Retryable(error.to_string())
    } else {
        DriverError::Permanent(error.to_string())
    }
}

fn classify_persistence(error: PersistenceError) -> DriverError {
    if error.is_conflict() {
        DriverError::Conflict
    } else {
        DriverError::Retryable(error.to_string())
    }
}

fn parse_batch_output(content: &str, custom_id: &str, model: &str) -> Result<Value, DriverError> {
    let mut saw_record = false;
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        saw_record = true;
        let value: Value = serde_json::from_str(line).map_err(|error| {
            DriverError::Permanent(format!("provider returned malformed batch JSONL: {error}"))
        })?;
        if value.get("custom_id").and_then(Value::as_str) != Some(custom_id) {
            continue;
        }
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            return Ok(failed_response(model, &error.to_string()));
        }
        let response = value.get("response").ok_or_else(|| {
            DriverError::Permanent("batch output record is missing response".to_string())
        })?;
        let status = response
            .get("status_code")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                DriverError::Permanent("batch output response is missing status_code".to_string())
            })?;
        let body = response.get("body").cloned().ok_or_else(|| {
            DriverError::Permanent("batch output response is missing body".to_string())
        })?;
        if !(200..300).contains(&status) {
            return Ok(failed_response(model, &body.to_string()));
        }
        if !body.is_object() {
            return Err(DriverError::Permanent(
                "batch response body is not a JSON object".to_string(),
            ));
        }
        return Ok(body);
    }
    let reason = if saw_record {
        format!("batch output does not contain custom_id {custom_id}")
    } else {
        "batch output is empty".to_string()
    };
    Err(DriverError::Permanent(reason))
}

fn normalize_batch_error(content: &str, custom_id: &str, model: &str) -> Value {
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("custom_id").and_then(Value::as_str) != Some(custom_id) {
            continue;
        }
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            return json!({
                "object": "response",
                "status": "failed",
                "model": model,
                "output": [],
                "error": error
            });
        }
        if let Some(body) = value
            .pointer("/response/body")
            .filter(|body| body.is_object())
        {
            return body.clone();
        }
    }
    failed_response(model, content)
}

fn failed_response(model: &str, message: &str) -> Value {
    json!({
        "object": "response",
        "status": "failed",
        "model": model,
        "output": [],
        "error": {
            "code": "kaiion_batch_failed",
            "message": message,
            "type": "server_error"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_only_the_matching_batch_output() {
        let content = concat!(
            "{\"custom_id\":\"other\",\"response\":null}\n",
            "{\"custom_id\":\"wanted\",\"response\":{\"status_code\":200,\"body\":{\"status\":\"completed\",\"output\":[]}},\"error\":null}\n"
        );
        assert_eq!(
            parse_batch_output(content, "wanted", "gpt-test").unwrap()["status"],
            "completed"
        );
    }

    #[test]
    fn missing_custom_id_is_a_protocol_failure() {
        let error =
            parse_batch_output("{\"custom_id\":\"other\"}\n", "wanted", "gpt-test").unwrap_err();
        assert!(matches!(error, DriverError::Permanent(_)));
    }
}
