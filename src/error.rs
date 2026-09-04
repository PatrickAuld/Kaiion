use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("{0}")]
    BadRequest(String),

    #[error("missing Authorization header")]
    Unauthorized,

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("upstream transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("upstream returned HTTP {status}: {body}")]
    Upstream { status: StatusCode, body: String },

    #[error("batch request failed: {0}")]
    BatchResult(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl ProxyError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "invalid_request",
            Self::Unauthorized => "unauthorized",
            Self::Upstream { .. } => "upstream_error",
            Self::BatchResult(_) => "batch_request_failed",
            Self::Transport(_) => "upstream_transport_error",
            Self::Database(_) | Self::Migration(_) | Self::Json(_) | Self::Internal(_) => {
                "internal_error"
            }
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Upstream { status, .. } => *status,
            Self::BatchResult(_) => StatusCode::BAD_GATEWAY,
            Self::Transport(_) => StatusCode::BAD_GATEWAY,
            Self::Database(_) | Self::Migration(_) | Self::Json(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Upstream { status, .. } => {
                status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
            _ => false,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = Json(json!({
            "error": {
                "message": self.to_string(),
                "type": "kaiion_error",
                "code": self.code()
            }
        }));
        (status, body).into_response()
    }
}
