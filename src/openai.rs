use axum::http::HeaderMap;
use reqwest::{RequestBuilder, multipart};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{error::ProxyError, request::UpstreamAuth};

const CONTROL_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone)]
pub struct OpenAiClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BatchObject {
    pub id: String,
    pub status: String,
    pub input_file_id: Option<String>,
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
            base_url: base_url.into().trim_end_matches('/').to_string(),
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
        job_id: &str,
        content: String,
    ) -> Result<String, ProxyError> {
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
        self.parse_json::<FileObject>(response).await.map(|file| file.id)
    }

    pub async fn create_batch(
        &self,
        auth: &UpstreamAuth,
        input_file_id: &str,
        job_id: &str,
        attempt: i64,
    ) -> Result<BatchObject, ProxyError> {
        let body = json!({
            "input_file_id": input_file_id,
            "endpoint": "/v1/responses",
            "completion_window": "24h",
            "metadata": {
                "kaiion_job_id": job_id,
                "kaiion_attempt": attempt.to_string()
            }
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
        batch_id: &str,
    ) -> Result<BatchObject, ProxyError> {
        let response = self
            .with_auth(
                self.http.get(self.url(&format!("batches/{batch_id}"))),
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
        job_id: &str,
        attempt: i64,
    ) -> Result<Option<BatchObject>, ProxyError> {
        let attempt = attempt.to_string();
        let mut after = None;

        // A restart can happen long after the batch was submitted. Search all
        // pages, not merely the most recent 100 batches, before creating a
        // replacement batch for the same durable job attempt.
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
                let Some(metadata) = batch.metadata.as_ref().and_then(Value::as_object) else {
                    return false;
                };
                metadata.get("kaiion_job_id").and_then(Value::as_str) == Some(job_id)
                    && metadata.get("kaiion_attempt").and_then(Value::as_str)
                        == Some(attempt.as_str())
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
    ) -> Result<String, ProxyError> {
        let response = self
            .with_auth(
                self.http
                    .get(self.url(&format!("files/{file_id}/content"))),
                auth,
            )
            .timeout(CONTROL_REQUEST_TIMEOUT)
            .send()
            .await?;
        self.parse_text(response).await
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
    ) -> Result<T, ProxyError> {
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ProxyError::Upstream { status, body });
        }
        Ok(response.json().await?)
    }

    async fn parse_text(&self, response: reqwest::Response) -> Result<String, ProxyError> {
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(ProxyError::Upstream { status, body });
        }
        Ok(body)
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
