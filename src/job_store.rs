use serde::Serialize;
use serde_json::Value;

#[derive(Serialize, sqlx::FromRow)]
pub struct JobSummary {
    pub id: String,
    pub model: String,
    pub status: String,
    pub terminal: bool,
}

use crate::{
    db::Database,
    domain::{Job, JobId},
    error::ProxyError,
    request::NormalizedRequest,
};

impl Database {
    pub async fn list_owned(
        &self,
        auth_hash: &str,
        provider: &str,
        after: &str,
        limit: u32,
    ) -> Result<Vec<JobSummary>, ProxyError> {
        let rows = sqlx::query_as::<_, JobSummary>("SELECT jobs.id, jobs.model, jobs.status, jobs.outcome_json IS NOT NULL AS terminal FROM jobs JOIN job_requests ON jobs.id = job_requests.job_id WHERE job_requests.auth_hash = ? AND job_requests.provider = ? AND jobs.id > ? ORDER BY jobs.id LIMIT ?")
            .bind(auth_hash).bind(provider).bind(after).bind(limit.min(100)).fetch_all(&self.pool).await?;
        Ok(rows)
    }
    pub async fn enqueue(
        &self,
        auth_hash: &str,
        provider: &str,
        request: &NormalizedRequest,
    ) -> Result<Job, ProxyError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO jobs (id, auth_hash, request_hash, model, status) VALUES (?, ?, ?, ?, 'queued')")
            .bind(uuid::Uuid::new_v4().simple().to_string())
            .bind(auth_hash).bind(&request.request_hash).bind(&request.model)
            .execute(&mut *transaction).await?;
        let id: String =
            sqlx::query_scalar("SELECT id FROM jobs WHERE auth_hash = ? AND request_hash = ?")
                .bind(auth_hash)
                .bind(&request.request_hash)
                .fetch_one(&mut *transaction)
                .await?;
        sqlx::query("INSERT OR IGNORE INTO job_requests (job_id, provider, auth_hash, idempotency_hash, body_json) VALUES (?, ?, ?, ?, ?)")
            .bind(&id).bind(provider).bind(auth_hash).bind(&request.idempotency_hash)
            .bind(serde_json::to_string(&request.batch_body)?)
            .execute(&mut *transaction).await?;
        if let Some(key) = &request.idempotency_hash {
            let owner: String = sqlx::query_scalar(
                "SELECT job_id FROM job_requests WHERE auth_hash = ? AND idempotency_hash = ?",
            )
            .bind(auth_hash)
            .bind(key)
            .fetch_one(&mut *transaction)
            .await?;
            if owner != id {
                return Err(ProxyError::Conflict("Idempotency-Key was already used with a different request; use a new key for a new inference".into()));
            }
        }
        transaction.commit().await?;
        Ok(self.get(&JobId(id)).await?)
    }

    pub async fn owned_job(
        &self,
        id: &JobId,
        auth_hash: &str,
        provider: &str,
    ) -> Result<Job, ProxyError> {
        let row = sqlx::query_as::<_, crate::db::RawJob>("SELECT jobs.id, jobs.model, jobs.status, jobs.input_file_id, jobs.batch_id, jobs.submission_started_at, jobs.outcome_json FROM jobs JOIN job_requests ON jobs.id = job_requests.job_id WHERE jobs.id = ? AND job_requests.auth_hash = ? AND job_requests.provider = ?")
            .bind(&id.0).bind(auth_hash).bind(provider).fetch_optional(&self.pool).await?.ok_or(ProxyError::NotFound)?;
        Ok(Job::try_from(row)?)
    }

    pub async fn owned_request(
        &self,
        id: &JobId,
        auth_hash: &str,
        provider: &str,
    ) -> Result<(Job, Value), ProxyError> {
        let body: String = sqlx::query_scalar("SELECT body_json FROM job_requests WHERE job_id = ? AND auth_hash = ? AND provider = ?")
            .bind(&id.0).bind(auth_hash).bind(provider).fetch_optional(&self.pool).await?
            .ok_or(ProxyError::NotFound)?;
        Ok((self.get(id).await?, serde_json::from_str(&body)?))
    }

    pub async fn check_idempotency(
        &self,
        auth_hash: &str,
        request: &NormalizedRequest,
    ) -> Result<(), ProxyError> {
        if let Some(key) = &request.idempotency_hash {
            let hash: Option<String> = sqlx::query_scalar("SELECT jobs.request_hash FROM job_requests JOIN jobs ON jobs.id = job_requests.job_id WHERE job_requests.auth_hash = ? AND job_requests.idempotency_hash = ?")
                .bind(auth_hash).bind(key).fetch_optional(&self.pool).await?;
            if hash.is_some_and(|hash| hash != request.request_hash) {
                return Err(ProxyError::Conflict(
                    "Idempotency-Key was already used with a different request".into(),
                ));
            }
        }
        Ok(())
    }
}
