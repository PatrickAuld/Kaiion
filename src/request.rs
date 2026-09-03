use axum::http::{HeaderMap, header::AUTHORIZATION};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{config::Mode, error::ProxyError};

pub const MODE_HEADER: &str = "x-kaiion-mode";

#[derive(Clone, Debug)]
pub struct UpstreamAuth {
    pub authorization: String,
    pub organization: Option<String>,
    pub project: Option<String>,
}

impl UpstreamAuth {
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, ProxyError> {
        let authorization = headers
            .get(AUTHORIZATION)
            .ok_or(ProxyError::Unauthorized)?
            .to_str()
            .map_err(|_| ProxyError::Unauthorized)?
            .to_string();
        let organization = optional_header(headers, "openai-organization")?;
        let project = optional_header(headers, "openai-project")?;
        Ok(Self {
            authorization,
            organization,
            project,
        })
    }

    pub fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        // Delimit and label each component. Without field labels, an
        // organization value and a project value with the same bytes would
        // share a durable job namespace.
        hash_component(&mut hasher, b"authorization", &self.authorization);
        if let Some(organization) = &self.organization {
            hash_component(&mut hasher, b"organization", organization);
        }
        if let Some(project) = &self.project {
            hash_component(&mut hasher, b"project", project);
        }
        hex_digest(hasher.finalize())
    }
}

#[derive(Clone, Debug)]
pub struct NormalizedRequest {
    pub batch_body: Value,
    pub request_hash: String,
    pub session_key: Option<String>,
    pub model: String,
    pub stream: bool,
}

impl NormalizedRequest {
    pub fn from_body(body: &Value, provider_namespace: &str) -> Result<Self, ProxyError> {
        let mut batch_body = body.clone();
        let object = batch_body.as_object_mut().ok_or_else(|| {
            ProxyError::BadRequest("Responses request must be a JSON object".to_string())
        })?;
        let model = object
            .get("model")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ProxyError::BadRequest("missing model".to_string()))?
            .to_string();
        let stream = object
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        object.insert("stream".to_string(), Value::Bool(false));
        object.remove("stream_options");

        let session_key = extract_session_key(object.get("client_metadata"));
        let mut fingerprint_body = batch_body.clone();
        remove_volatile_codex_metadata(&mut fingerprint_body);
        let encoded = serde_json::to_vec(&fingerprint_body)?;
        let mut hasher = Sha256::new();
        // A persisted database can be reused after configuration changes.
        // Scope durable identities to the configured provider so an identical
        // request is never replayed from a different upstream deployment.
        hash_component(&mut hasher, b"provider", provider_namespace);
        hash_bytes(&mut hasher, b"request", &encoded);
        let request_hash = hex_digest(hasher.finalize());

        Ok(Self {
            batch_body,
            request_hash,
            session_key,
            model,
            stream,
        })
    }
}

pub fn resolve_mode(headers: &HeaderMap, default: Mode) -> Result<Mode, ProxyError> {
    let Some(value) = headers.get(MODE_HEADER) else {
        return Ok(default);
    };
    match value.to_str().unwrap_or_default() {
        "batch" => Ok(Mode::Batch),
        "direct" => Ok(Mode::Direct),
        value => Err(ProxyError::BadRequest(format!(
            "invalid {MODE_HEADER} value {value:?}; expected batch or direct"
        ))),
    }
}

fn optional_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, ProxyError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| ProxyError::BadRequest(format!("invalid {name} header")))
        })
        .transpose()
}

fn extract_session_key(client_metadata: Option<&Value>) -> Option<String> {
    let metadata = client_metadata?.as_object()?;
    let thread = metadata
        .get("thread_id")
        .and_then(Value::as_str)
        .or_else(|| metadata.get("session_id").and_then(Value::as_str))?;
    let turn = metadata.get("turn_id").and_then(Value::as_str).unwrap_or("-");
    let mut hasher = Sha256::new();
    hasher.update(thread.as_bytes());
    hasher.update([0]);
    hasher.update(turn.as_bytes());
    Some(hex_digest(hasher.finalize()))
}

fn remove_volatile_codex_metadata(body: &mut Value) {
    let Some(metadata) = body
        .get_mut("client_metadata")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    metadata.remove("x-codex-window-id");
    metadata.remove("x-codex-turn-metadata");
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn hash_component(hasher: &mut Sha256, name: &[u8], value: &str) {
    hash_bytes(hasher, name, value.as_bytes());
}

fn hash_bytes(hasher: &mut Sha256, name: &[u8], value: &[u8]) {
    hasher.update((name.len() as u64).to_be_bytes());
    hasher.update(name);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn normalizes_streaming_and_volatile_metadata() {
        let first = json!({
            "model": "gpt-test",
            "stream": true,
            "stream_options": {"include_usage": true},
            "input": [{"role": "user", "content": "hello"}],
            "client_metadata": {
                "thread_id": "thread",
                "turn_id": "turn",
                "x-codex-window-id": "window-1",
                "x-codex-turn-metadata": "volatile-1"
            }
        });
        let second = json!({
            "model": "gpt-test",
            "stream": false,
            "input": [{"role": "user", "content": "hello"}],
            "client_metadata": {
                "thread_id": "thread",
                "turn_id": "turn",
                "x-codex-window-id": "window-2",
                "x-codex-turn-metadata": "volatile-2"
            }
        });
        let first = NormalizedRequest::from_body(&first, "provider-a").unwrap();
        let second = NormalizedRequest::from_body(&second, "provider-a").unwrap();
        assert_eq!(first.request_hash, second.request_hash);
        assert_eq!(first.session_key, second.session_key);
        assert_eq!(first.batch_body["stream"], false);
        assert!(first.batch_body.get("stream_options").is_none());
    }

    #[test]
    fn preserves_turn_identity_in_fingerprint() {
        let first = json!({
            "model": "gpt-test",
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn-1"}
        });
        let second = json!({
            "model": "gpt-test",
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn-2"}
        });
        assert_ne!(
            NormalizedRequest::from_body(&first, "provider-a")
                .unwrap()
                .request_hash,
            NormalizedRequest::from_body(&second, "provider-a")
                .unwrap()
                .request_hash
        );
    }

    #[test]
    fn scopes_request_identity_to_the_provider() {
        let body = json!({
            "model": "gpt-test",
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn"}
        });
        assert_ne!(
            NormalizedRequest::from_body(&body, "provider-a")
                .unwrap()
                .request_hash,
            NormalizedRequest::from_body(&body, "provider-b")
                .unwrap()
                .request_hash
        );
    }
}
