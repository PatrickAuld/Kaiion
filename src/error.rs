use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

use crate::db::PersistenceError;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("{0}")]
    BadRequest(String),

    #[error("missing Authorization header")]
    Unauthorized,

    #[error(transparent)]
    Persistence(#[from] PersistenceError),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("upstream transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl ProxyError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "invalid_request",
            Self::Unauthorized => "unauthorized",
            Self::Transport(_) => "upstream_transport_error",
            Self::Persistence(_)
            | Self::Database(_)
            | Self::Migration(_)
            | Self::Json(_)
            | Self::Internal(_) => "internal_error",
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Transport(_) => StatusCode::BAD_GATEWAY,
            Self::Persistence(_)
            | Self::Database(_)
            | Self::Migration(_)
            | Self::Json(_)
            | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
