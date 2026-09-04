use std::{str::FromStr, time::Duration};

use serde_json::Value;
use sqlx::{
    FromRow, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    domain::{BatchId, FileId, Job, JobId, JobState, StoredOutcome, Timestamp},
    error::ProxyError,
};

#[derive(Clone)]
pub struct Database {
    pub(crate) pool: SqlitePool,
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid persisted job {job_id}: {reason}")]
    InvalidJob { job_id: String, reason: String },
    #[error("job {job_id} transition conflict; expected {expected}")]
    TransitionConflict {
        job_id: String,
        expected: &'static str,
    },
    #[error("job {0} not found")]
    NotFound(String),
}

impl PersistenceError {
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::TransitionConflict { .. })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct RawJob {
    id: String,
    model: String,
    status: String,
    input_file_id: Option<String>,
    batch_id: Option<String>,
    submission_started_at: Option<String>,
    outcome_json: Option<String>,
}

impl TryFrom<RawJob> for Job {
    type Error = PersistenceError;

    fn try_from(row: RawJob) -> Result<Self, Self::Error> {
        let invalid = |reason: &str| PersistenceError::InvalidJob {
            job_id: row.id.clone(),
            reason: reason.to_string(),
        };
        let reject = |present: bool, column: &str| {
            if present {
                Err(invalid(&format!("unexpected {column}")))
            } else {
                Ok(())
            }
        };
        let state = match row.status.as_str() {
            "queued" => {
                reject(row.input_file_id.is_some(), "input_file_id")?;
                reject(row.batch_id.is_some(), "batch_id")?;
                reject(row.submission_started_at.is_some(), "submission_started_at")?;
                reject(row.outcome_json.is_some(), "outcome_json")?;
                JobState::Queued
            }
            "uploaded" => {
                reject(row.batch_id.is_some(), "batch_id")?;
                reject(row.submission_started_at.is_some(), "submission_started_at")?;
                reject(row.outcome_json.is_some(), "outcome_json")?;
                JobState::Uploaded {
                    input_file_id: FileId(
                        row.input_file_id
                            .clone()
                            .ok_or_else(|| invalid("missing input_file_id"))?,
                    ),
                }
            }
            "submitting" | "submission_uncertain" => {
                reject(row.batch_id.is_some(), "batch_id")?;
                reject(row.outcome_json.is_some(), "outcome_json")?;
                let input_file_id = FileId(
                    row.input_file_id
                        .clone()
                        .ok_or_else(|| invalid("missing input_file_id"))?,
                );
                let started_at = Timestamp(
                    row.submission_started_at
                        .clone()
                        .ok_or_else(|| invalid("missing submission_started_at"))?,
                );
                if row.status == "submitting" {
                    JobState::Submitting {
                        input_file_id,
                        started_at,
                    }
                } else {
                    JobState::SubmissionUncertain {
                        input_file_id,
                        started_at,
                    }
                }
            }
            "submitted" => {
                reject(row.input_file_id.is_some(), "input_file_id")?;
                reject(row.submission_started_at.is_some(), "submission_started_at")?;
                reject(row.outcome_json.is_some(), "outcome_json")?;
                JobState::Submitted {
                    batch_id: BatchId(
                        row.batch_id
                            .clone()
                            .ok_or_else(|| invalid("missing batch_id"))?,
                    ),
                }
            }
            "completed" | "failed" | "incomplete" | "expired" | "cancelled" => {
                reject(row.input_file_id.is_some(), "input_file_id")?;
                reject(row.batch_id.is_some(), "batch_id")?;
                reject(row.submission_started_at.is_some(), "submission_started_at")?;
                let encoded = row
                    .outcome_json
                    .as_deref()
                    .ok_or_else(|| invalid("missing outcome_json"))?;
                let value = serde_json::from_str::<Value>(encoded)
                    .map_err(|error| invalid(&format!("invalid outcome_json: {error}")))?;
                JobState::Terminal(match row.status.as_str() {
                    "completed" => StoredOutcome::Completed(value),
                    "failed" => StoredOutcome::Failed(value),
                    "incomplete" => StoredOutcome::Incomplete(value),
                    "expired" => StoredOutcome::Expired(value),
                    "cancelled" => StoredOutcome::Cancelled(value),
                    _ => unreachable!(),
                })
            }
            status => return Err(invalid(&format!("unknown status {status:?}"))),
        };
        Ok(Job {
            id: JobId(row.id),
            model: row.model,
            state,
        })
    }
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self, ProxyError> {
        let mut options = SqliteConnectOptions::from_str(database_url)
            .map_err(|error| ProxyError::Internal(format!("invalid SQLite database URL: {error}")))?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let max_connections = if database_url.contains(":memory:") {
            1
        } else {
            options = options.journal_mode(SqliteJournalMode::Wal);
            5
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn get_or_create(
        &self,
        auth_hash: &str,
        request_hash: &str,
        model: &str,
    ) -> Result<Job, PersistenceError> {
        let id = Uuid::new_v4().simple().to_string();
        sqlx::query(
            "INSERT OR IGNORE INTO jobs (id, auth_hash, request_hash, model, status) VALUES (?, ?, ?, ?, 'queued')",
        )
        .bind(id)
        .bind(auth_hash)
        .bind(request_hash)
        .bind(model)
        .execute(&self.pool)
        .await?;
        self.find(auth_hash, request_hash)
            .await?
            .ok_or_else(|| PersistenceError::NotFound("new job".to_string()))
    }

    pub async fn get(&self, id: &JobId) -> Result<Job, PersistenceError> {
        self.fetch_optional("SELECT id, model, status, input_file_id, batch_id, submission_started_at, outcome_json FROM jobs WHERE id = ?", &id.0)
            .await?
            .ok_or_else(|| PersistenceError::NotFound(id.0.clone()))
    }

    pub async fn find(
        &self,
        auth_hash: &str,
        request_hash: &str,
    ) -> Result<Option<Job>, PersistenceError> {
        let row = sqlx::query_as::<_, RawJob>(
            "SELECT id, model, status, input_file_id, batch_id, submission_started_at, outcome_json FROM jobs WHERE auth_hash = ? AND request_hash = ?",
        )
        .bind(auth_hash)
        .bind(request_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    async fn fetch_optional(&self, query: &str, id: &str) -> Result<Option<Job>, PersistenceError> {
        sqlx::query_as::<_, RawJob>(query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(TryInto::try_into)
            .transpose()
    }

    pub async fn mark_uploaded(
        &self,
        id: &JobId,
        input_file_id: FileId,
    ) -> Result<JobState, PersistenceError> {
        self.transition(
            sqlx::query_as::<_, RawJob>(
                r#"UPDATE jobs SET status = 'uploaded', input_file_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                   WHERE id = ? AND status = 'queued'
                   RETURNING id, model, status, input_file_id, batch_id, submission_started_at, outcome_json"#,
            )
            .bind(input_file_id.0)
            .bind(&id.0),
            id,
            "queued",
        )
        .await
    }

    pub async fn begin_submission(
        &self,
        id: &JobId,
        input_file_id: &FileId,
    ) -> Result<JobState, PersistenceError> {
        self.transition(
            sqlx::query_as::<_, RawJob>(
                r#"UPDATE jobs SET status = 'submitting', submission_started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                   WHERE id = ? AND status = 'uploaded' AND input_file_id = ?
                   RETURNING id, model, status, input_file_id, batch_id, submission_started_at, outcome_json"#,
            )
            .bind(&id.0)
            .bind(&input_file_id.0),
            id,
            "uploaded",
        )
        .await
    }

    pub async fn mark_submission_uncertain(
        &self,
        id: &JobId,
    ) -> Result<JobState, PersistenceError> {
        self.transition(
            sqlx::query_as::<_, RawJob>(
                r#"UPDATE jobs SET status = 'submission_uncertain', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                   WHERE id = ? AND status = 'submitting'
                   RETURNING id, model, status, input_file_id, batch_id, submission_started_at, outcome_json"#,
            )
            .bind(&id.0),
            id,
            "submitting",
        )
        .await
    }

    pub async fn mark_submitted(
        &self,
        id: &JobId,
        expected: &JobState,
        batch_id: BatchId,
    ) -> Result<JobState, PersistenceError> {
        let expected_status = status_of(expected);
        self.transition(
            sqlx::query_as::<_, RawJob>(
                r#"UPDATE jobs SET status = 'submitted', input_file_id = NULL, submission_started_at = NULL, batch_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                   WHERE id = ? AND status = ?
                   RETURNING id, model, status, input_file_id, batch_id, submission_started_at, outcome_json"#,
            )
            .bind(batch_id.0)
            .bind(&id.0)
            .bind(expected_status),
            id,
            expected_status,
        )
        .await
    }

    pub async fn store_outcome(
        &self,
        id: &JobId,
        expected: &JobState,
        outcome: StoredOutcome,
    ) -> Result<JobState, PersistenceError> {
        let expected_status = status_of(expected);
        let (status, value) = encode_outcome(&outcome);
        let outcome_json =
            serde_json::to_string(value).map_err(|error| PersistenceError::InvalidJob {
                job_id: id.0.clone(),
                reason: format!("cannot encode terminal outcome: {error}"),
            })?;
        self.transition(
            sqlx::query_as::<_, RawJob>(
                r#"UPDATE jobs SET status = ?, input_file_id = NULL, batch_id = NULL, submission_started_at = NULL, outcome_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                   WHERE id = ? AND status = ?
                   RETURNING id, model, status, input_file_id, batch_id, submission_started_at, outcome_json"#,
            )
            .bind(status)
            .bind(outcome_json)
            .bind(&id.0)
            .bind(expected_status),
            id,
            expected_status,
        )
        .await
    }

    async fn transition<'q>(
        &self,
        query: sqlx::query::QueryAs<'q, sqlx::Sqlite, RawJob, sqlx::sqlite::SqliteArguments<'q>>,
        id: &JobId,
        expected: &'static str,
    ) -> Result<JobState, PersistenceError> {
        let row = query.fetch_optional(&self.pool).await?.ok_or_else(|| {
            PersistenceError::TransitionConflict {
                job_id: id.0.clone(),
                expected,
            }
        })?;
        Ok(Job::try_from(row)?.state)
    }
}

fn status_of(state: &JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Uploaded { .. } => "uploaded",
        JobState::Submitting { .. } => "submitting",
        JobState::SubmissionUncertain { .. } => "submission_uncertain",
        JobState::Submitted { .. } => "submitted",
        JobState::Terminal(StoredOutcome::Completed(_)) => "completed",
        JobState::Terminal(StoredOutcome::Failed(_)) => "failed",
        JobState::Terminal(StoredOutcome::Incomplete(_)) => "incomplete",
        JobState::Terminal(StoredOutcome::Expired(_)) => "expired",
        JobState::Terminal(StoredOutcome::Cancelled(_)) => "cancelled",
    }
}

fn encode_outcome(outcome: &StoredOutcome) -> (&'static str, &Value) {
    match outcome {
        StoredOutcome::Completed(value) => ("completed", value),
        StoredOutcome::Failed(value) => ("failed", value),
        StoredOutcome::Incomplete(value) => ("incomplete", value),
        StoredOutcome::Expired(value) => ("expired", value),
        StoredOutcome::Cancelled(value) => ("cancelled", value),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn reuses_an_existing_request() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let first = db.get_or_create("auth", "request", "model").await.unwrap();
        let second = db.get_or_create("auth", "request", "model").await.unwrap();
        assert_eq!(first.id, second.id);
    }

    #[tokio::test]
    async fn transitions_are_typed_and_conflict_aware() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let job = db.get_or_create("auth", "request", "model").await.unwrap();
        let uploaded = db
            .mark_uploaded(&job.id, FileId("file-1".to_string()))
            .await
            .unwrap();
        assert!(matches!(uploaded, JobState::Uploaded { .. }));
        let conflict = db
            .mark_uploaded(&job.id, FileId("file-2".to_string()))
            .await
            .unwrap_err();
        assert!(conflict.is_conflict());
    }

    #[tokio::test]
    async fn terminal_payload_and_state_are_stored_together() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let job = db.get_or_create("auth", "request", "model").await.unwrap();
        let state = db
            .store_outcome(
                &job.id,
                &job.state,
                StoredOutcome::Failed(json!({"message": "failure"})),
            )
            .await
            .unwrap();
        assert!(matches!(
            state,
            JobState::Terminal(StoredOutcome::Failed(_))
        ));
        assert_eq!(db.get(&job.id).await.unwrap().state, state);
    }

    #[tokio::test]
    async fn migrates_a_populated_version_one_database() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("version-one.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let pool = SqlitePool::connect(&url).await.unwrap();
        sqlx::raw_sql(include_str!("../migrations/0001_init.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"INSERT INTO jobs (id, auth_hash, request_hash, model, status, result_json)
               VALUES ('legacy', 'auth', 'request', 'model', 'completed', '{"status":"completed","output":[]}')"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let db = Database::connect(&url).await.unwrap();
        let job = db.get(&JobId("legacy".to_string())).await.unwrap();
        assert!(matches!(
            job.state,
            JobState::Terminal(StoredOutcome::Completed(_))
        ));
    }
}
