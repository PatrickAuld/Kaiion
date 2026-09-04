use axum::http::{HeaderMap, StatusCode};
use reqwest::{RequestBuilder, multipart};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    domain::{BatchId, FileId, JobId},
    error::ProxyError,
    request::{UpstreamAuth, canonical_provider_url},
};

const CONTROL_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("provider transport failure: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("provider returned retryable HTTP {status}: {body}")]
    RetryableHttp { status: StatusCode, body: String },
    #[error("provider returned permanent HTTP {status}: {body}")]
    PermanentHttp { status: StatusCode, body: String },
    #[error("provider protocol violation: {0}")]
    Protocol(String),
    #[error("provider output file is not visible yet (HTTP {status}): {body}")]
    OutputNotVisible { status: StatusCode, body: String },
}

impl ProviderError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport(_) | Self::RetryableHttp { .. } | Self::OutputNotVisible { .. }
        )
    }
}

impl From<reqwest::Error> for ProviderError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error)
    }
}

#[derive(Clone)]
pub struct OpenAiClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BatchObject {
    pub id: String,
    pub status: String,
    pub output_file_id: Option<String>,
    pub error_file_id: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct FileObject {
    id: String,
}

#[derive(Debug, Deserialize)]
struct BatchList {
    data: Vec<BatchObject>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    last_id: Option<String>,
}

impl OpenAiClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self, ProxyError> {
        let http = reqwest::Client::builder()
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(15))
            .build()?;
        Ok(Self {
            http,
            base_url: canonical_provider_url(&base_url.into())?,
        })
    }

    pub async fn direct(
        &self,
        headers: &HeaderMap,
        body: &Value,
    ) -> Result<reqwest::Response, ProxyError> {
        let mut request = self
            .http
            .post(self.url("responses"))
            .header("content-type", "application/json");
        for (name, value) in headers {
            if should_forward_request_header(name.as_str()) {
                request = request.header(name, value);
            }
        }
        Ok(request.json(body).send().await?)
    }

    pub async fn upload_batch_file(
        &self,
        auth: &UpstreamAuth,
        job_id: &JobId,
        content: String,
    ) -> Result<FileId, ProviderError> {
        let part = multipart::Part::bytes(content.into_bytes())
            .file_name(format!("kaiion-{job_id}.jsonl"))
            .mime_str("application/jsonl")?;
        let form = multipart::Form::new()
            .text("purpose", "batch")
            .part("file", part);
        let response = self
            .with_auth(self.http.post(self.url("files")), auth)
            .multipart(form)
            .timeout(CONTROL_REQUEST_TIMEOUT)
            .send()
            .await?;
        self.parse_json::<FileObject>(response)
            .await
            .map(|file| FileId(file.id))
    }

    pub async fn create_batch(
        &self,
        auth: &UpstreamAuth,
        input_file_id: &FileId,
        job_id: &JobId,
    ) -> Result<BatchObject, ProviderError> {
        let body = json!({
            "input_file_id": input_file_id.0,
            "endpoint": "/v1/responses",
            "completion_window": "24h",
            "metadata": {"kaiion_job_id": job_id.0}
        });
        let response = self
            .with_auth(self.http.post(self.url("batches")), auth)
            .json(&body)
            .timeout(CONTROL_REQUEST_TIMEOUT)
            .send()
            .await?;
        self.parse_json(response).await
    }

    pub async fn get_batch(
        &self,
        auth: &UpstreamAuth,
        batch_id: &BatchId,
    ) -> Result<BatchObject, ProviderError> {
        let response = self
            .with_auth(
                self.http.get(self.url(&format!("batches/{}", batch_id.0))),
                auth,
            )
            .timeout(CONTROL_REQUEST_TIMEOUT)
            .send()
            .await?;
        self.parse_json(response).await
    }

    pub async fn find_batch(
        &self,
        auth: &UpstreamAuth,
        job_id: &JobId,
    ) -> Result<Option<BatchObject>, ProviderError> {
        let mut after = None;
        loop {
            let mut request = self
                .with_auth(self.http.get(self.url("batches")), auth)
                .query(&[("limit", "100")]);
            if let Some(last_id) = &after {
                request = request.query(&[("after", last_id)]);
            }
            let response = request.timeout(CONTROL_REQUEST_TIMEOUT).send().await?;
            let batches: BatchList = self.parse_json(response).await?;
            if let Some(batch) = batches.data.into_iter().find(|batch| {
                batch
                    .metadata
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|metadata| metadata.get("kaiion_job_id"))
                    .and_then(Value::as_str)
                    == Some(job_id.0.as_str())
            }) {
                return Ok(Some(batch));
            }
            let Some(last_id) = batches.last_id.filter(|id| !id.is_empty()) else {
                return Ok(None);
            };
            if !batches.has_more || after.as_deref() == Some(last_id.as_str()) {
                return Ok(None);
            }
            after = Some(last_id);
        }
    }

    pub async fn get_file_content(
        &self,
        auth: &UpstreamAuth,
        file_id: &str,
    ) -> Result<String, ProviderError> {
        let response = self
            .with_auth(
                self.http.get(self.url(&format!("files/{file_id}/content"))),
                auth,
            )
            .timeout(CONTROL_REQUEST_TIMEOUT)
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        let body = String::from_utf8(bytes.to_vec())
            .map_err(|error| ProviderError::Protocol(format!("non-UTF-8 file content: {error}")))?;
        if status == StatusCode::NOT_FOUND || status == StatusCode::CONFLICT {
            return Err(ProviderError::OutputNotVisible { status, body });
        }
        if status.is_success() {
            return Ok(body);
        }
        classify_status(status, body)
    }

    fn with_auth(&self, mut request: RequestBuilder, auth: &UpstreamAuth) -> RequestBuilder {
        request = request.header("authorization", &auth.authorization);
        if let Some(organization) = &auth.organization {
            request = request.header("openai-organization", organization);
        }
        if let Some(project) = &auth.project {
            request = request.header("openai-project", project);
        }
        request
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    async fn parse_json<T: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, ProviderError> {
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            return classify_status(status, body);
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| ProviderError::Protocol(format!("invalid JSON payload: {error}")))
    }
}

fn classify_status<T>(status: StatusCode, body: String) -> Result<T, ProviderError> {
    if status.is_success() {
        return Err(ProviderError::Protocol(
            "successful response could not be decoded".to_string(),
        ));
    }
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        Err(ProviderError::RetryableHttp { status, body })
    } else {
        Err(ProviderError::PermanentHttp { status, body })
    }
}

fn should_forward_request_header(name: &str) -> bool {
    !matches!(
        name,
        "connection"
            | "content-length"
            | "content-type"
            | "host"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "x-kaiion-mode"
    )
}

pub fn copy_response_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if !matches!(
            name.as_str(),
            "connection" | "content-length" | "transfer-encoding" | "upgrade"
        ) {
            target.append(name.clone(), value.clone());
        }
    }
}
