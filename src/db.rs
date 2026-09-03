use std::{str::FromStr, time::Duration};

use sqlx::{
    FromRow, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use uuid::Uuid;

use crate::error::ProxyError;

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
}

#[derive(Clone, Debug, FromRow)]
pub struct Job {
    pub id: String,
    pub auth_hash: String,
    pub request_hash: String,
    pub session_key: Option<String>,
    pub model: String,
    pub status: String,
    pub attempt: i64,
    pub input_file_id: Option<String>,
    pub batch_id: Option<String>,
    pub output_file_id: Option<String>,
    pub result_json: Option<String>,
    pub error_json: Option<String>,
    pub delivered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Job {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "completed" | "failed" | "expired" | "cancelled"
        )
    }

    pub fn custom_id(&self) -> String {
        format!("kaiion-{}-{}", self.id, self.attempt)
    }
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self, ProxyError> {
        let mut options = SqliteConnectOptions::from_str(database_url)
            .map_err(|error| ProxyError::Internal(format!("invalid SQLite database URL: {error}")))?
            .create_if_missing(true)
            .foreign_keys(true)
            // Several clients may replay the same turn after a restart. Give
            // SQLite a chance to serialize those short writes instead of
            // failing them immediately with "database is locked".
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
        session_key: Option<&str>,
        model: &str,
    ) -> Result<Job, ProxyError> {
        let id = Uuid::new_v4().simple().to_string();
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO jobs (
                id, auth_hash, request_hash, session_key, model, status
            ) VALUES (?, ?, ?, ?, ?, 'queued')
            "#,
        )
        .bind(id)
        .bind(auth_hash)
        .bind(request_hash)
        .bind(session_key)
        .bind(model)
        .execute(&self.pool)
        .await?;

        let job = self
            .find(auth_hash, request_hash)
            .await?
            .ok_or_else(|| ProxyError::Internal("failed to create job".to_string()))?;

        // Terminal jobs are durable records. A Codex restart requesting the
        // same turn must replay its completed or failed result, never submit
        // a second paid Batch implicitly.
        Ok(job)
    }

    pub async fn get(&self, id: &str) -> Result<Job, ProxyError> {
        sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| ProxyError::Internal(format!("job {id} not found")))
    }

    pub async fn find(
        &self,
        auth_hash: &str,
        request_hash: &str,
    ) -> Result<Option<Job>, ProxyError> {
        Ok(sqlx::query_as::<_, Job>(
            "SELECT * FROM jobs WHERE auth_hash = ? AND request_hash = ?",
        )
        .bind(auth_hash)
        .bind(request_hash)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn mark_other_requests_delivered(
        &self,
        auth_hash: &str,
        session_key: Option<&str>,
        request_hash: &str,
    ) -> Result<(), ProxyError> {
        let Some(session_key) = session_key else {
            return Ok(());
        };
        sqlx::query(
            r#"
            UPDATE jobs
            SET delivered_at = COALESCE(
                    delivered_at,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                ),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE auth_hash = ?
              AND session_key = ?
              AND request_hash != ?
              AND status = 'completed'
            "#,
        )
        .bind(auth_hash)
        .bind(session_key)
        .bind(request_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_file_uploaded(
        &self,
        id: &str,
        input_file_id: &str,
    ) -> Result<bool, ProxyError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'file_uploaded', input_file_id = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'queued'
            "#,
        )
        .bind(input_file_id)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Persist the intent to create a Batch before issuing the upstream
    /// request. If the process dies after the request leaves the socket, a
    /// replay can discover the batch by metadata instead of blindly creating
    /// another one.
    pub async fn mark_submitting(&self, id: &str, attempt: i64) -> Result<bool, ProxyError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'submitting',
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND attempt = ? AND status = 'file_uploaded'
            "#,
        )
        .bind(id)
        .bind(attempt)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn mark_submitted(&self, id: &str, batch_id: &str) -> Result<(), ProxyError> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'submitted', batch_id = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'submitting'
            "#,
        )
        .bind(batch_id)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_completed(
        &self,
        id: &str,
        output_file_id: Option<&str>,
        result_json: &str,
    ) -> Result<(), ProxyError> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'completed', output_file_id = ?, result_json = ?, error_json = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'submitted'
            "#,
        )
        .bind(output_file_id)
        .bind(result_json)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_failed(
        &self,
        id: &str,
        status: &str,
        error_json: &str,
    ) -> Result<(), ProxyError> {
        let status = match status {
            "expired" => "expired",
            "cancelled" => "cancelled",
            _ => "failed",
        };
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = ?, error_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status IN ('queued', 'file_uploaded', 'submitting', 'submitted')
            "#,
        )
        .bind(status)
        .bind(error_json)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_delivered(&self, id: &str) -> Result<(), ProxyError> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET delivered_at = COALESCE(
                    delivered_at,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                ),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ?
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reuses_an_existing_request() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let first = db
            .get_or_create("auth", "request", Some("session"), "model")
            .await
            .unwrap();
        let second = db
            .get_or_create("auth", "request", Some("session"), "model")
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
    }

    #[tokio::test]
    async fn replays_a_failed_request_without_a_new_attempt() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let first = db
            .get_or_create("auth", "request", Some("session"), "model")
            .await
            .unwrap();
        db.mark_failed(&first.id, "failed", "failure")
            .await
            .unwrap();
        let second = db
            .get_or_create("auth", "request", Some("session"), "model")
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(second.attempt, 1);
        assert_eq!(second.status, "failed");
    }
}
