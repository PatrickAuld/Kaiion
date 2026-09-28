use axum::http::{HeaderMap, header::AUTHORIZATION};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{config::Mode, error::ProxyError};

pub const MODE_HEADER: &str = "x-kaiion-mode";
pub const SESSION_HEADER: &str = "x-kaiion-session-id";
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";

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
    pub model: String,
    pub async_alias: bool,
    pub idempotency_hash: Option<String>,
}

/// Whether an Auto request can be represented by the durable Batch path. A
/// missing stable identity or Batch-only unsupported field is a reason to keep
/// the request live; malformed Kaiion identity headers remain errors.
pub fn batch_supported(body: &Value, headers: &HeaderMap) -> Result<bool, ProxyError> {
    let Some(object) = body.as_object() else {
        return Ok(false);
    };
    let Some(requested_model) = object
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };
    if model_alias(requested_model).0.is_empty()
        || object
            .get("stream")
            .is_some_and(|value| !value.is_boolean())
        || object
            .get("store")
            .is_some_and(|value| value != &Value::Bool(false))
        || object
            .get("previous_response_id")
            .is_some_and(|value| !value.is_null())
        || object
            .get("conversation")
            .is_some_and(|value| !value.is_null())
        || object
            .get("background")
            .is_some_and(|value| value != &Value::Bool(false))
    {
        return Ok(false);
    }
    let session = identity_header(headers, SESSION_HEADER)?;
    let idempotency = identity_header(headers, IDEMPOTENCY_HEADER)?;
    Ok(session.is_some()
        || extract_session_key(object.get("client_metadata")).is_some()
        || idempotency.is_some())
}

/// Returns the provider model name and whether the caller used Kaiion's
/// wait-tolerant virtual alias.
pub fn model_alias(model: &str) -> (&str, bool) {
    match model.strip_prefix("async-") {
        Some(model) => (model, true),
        None => (model, false),
    }
}

/// Make a provider-bound copy of the request, removing Kaiion's virtual model
/// prefix without changing the caller's request.
pub fn upstream_body(body: &Value) -> Result<Value, ProxyError> {
    let mut upstream = body.clone();
    let object = upstream.as_object_mut().ok_or_else(|| {
        ProxyError::BadRequest("Responses request must be a JSON object".to_string())
    })?;
    if let Some(model) = object.get("model").and_then(Value::as_str) {
        let (model, alias) = model_alias(model);
        if alias && model.is_empty() {
            return Err(ProxyError::BadRequest(
                "async- model alias must include an upstream model name".into(),
            ));
        }
        object.insert("model".into(), Value::String(model.to_string()));
    }
    Ok(upstream)
}

/// Stable, process-local session key for scheduling. This is separate from
/// batch request identity, which includes the turn id and is persisted.
pub fn scheduling_session_id(
    body: &Value,
    headers: &HeaderMap,
) -> Result<Option<String>, ProxyError> {
    let session = identity_header(headers, SESSION_HEADER)?
        .or_else(|| extract_thread_id(body.get("client_metadata")));
    Ok(session.map(|value| {
        let mut hasher = Sha256::new();
        hash_component(&mut hasher, b"scheduler-session", &value);
        hex_digest(hasher.finalize())
    }))
}

pub fn estimate_tokens(body: &Value) -> (u64, u64) {
    let bytes: usize = ["instructions", "input", "tools", "text"]
        .iter()
        .filter_map(|key| body.get(key))
        .map(|value| value.to_string().len())
        .sum();
    let input = (bytes as u64).div_ceil(3).saturating_add(32);
    let output = body
        .get("max_output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (input, output)
}

pub fn is_tool_output(body: &Value) -> bool {
    contains_item_type(
        latest_input(body),
        &["function_call_output", "tool_result", "tool_output"],
    )
}

pub fn is_user_prompt(body: &Value) -> bool {
    if is_tool_output(body) {
        return false;
    }
    if body.get("input").is_some_and(Value::is_string) {
        return true;
    }
    contains_user_marker(latest_input(body))
}

fn latest_input(body: &Value) -> Option<&Value> {
    match body.get("input") {
        Some(Value::Array(items)) => items.last(),
        value => value,
    }
}

pub fn response_usage(value: &Value) -> (Option<u64>, Option<u64>, Option<bool>) {
    let usage = value
        .get("response")
        .and_then(Value::as_object)
        .and_then(|response| response.get("usage"))
        .or_else(|| value.get("usage"));
    let Some(usage) = usage else {
        return (None, None, None);
    };
    let input = usage.get("input_tokens").and_then(Value::as_u64);
    let output = usage.get("output_tokens").and_then(Value::as_u64);
    let cached = usage
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .map(|tokens| tokens > 0);
    (input, output, cached)
}

impl NormalizedRequest {
    pub fn from_body(body: &Value, provider_namespace: &str) -> Result<Self, ProxyError> {
        Self::from_headers(body, provider_namespace, &HeaderMap::new())
    }

    pub fn from_headers(
        body: &Value,
        provider_namespace: &str,
        headers: &HeaderMap,
    ) -> Result<Self, ProxyError> {
        let mut batch_body = body.clone();
        let object = batch_body.as_object_mut().ok_or_else(|| {
            ProxyError::BadRequest("Responses request must be a JSON object".to_string())
        })?;
        let requested_model = object
            .get("model")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ProxyError::BadRequest("missing model".to_string()))?
            .to_string();
        let (model, async_alias) = model_alias(&requested_model);
        if model.is_empty() {
            return Err(ProxyError::BadRequest("missing model".to_string()));
        }
        let model = model.to_string();
        object.insert("model".into(), Value::String(model.clone()));
        if object
            .get("stream")
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(ProxyError::BadRequest(
                "stream must be a boolean".to_string(),
            ));
        }
        if object
            .get("store")
            .is_some_and(|value| value != &Value::Bool(false))
        {
            return Err(ProxyError::BadRequest(
                "batch mode does not support store=true".to_string(),
            ));
        }
        if object
            .get("previous_response_id")
            .is_some_and(|value| !value.is_null())
        {
            return Err(ProxyError::BadRequest(
                "batch mode does not support previous_response_id".to_string(),
            ));
        }

        if object
            .get("conversation")
            .is_some_and(|value| !value.is_null())
            || object
                .get("background")
                .is_some_and(|value| value != &Value::Bool(false))
        {
            return Err(ProxyError::BadRequest(
                "batch mode does not support conversation or background=true".into(),
            ));
        }

        let session = identity_header(headers, SESSION_HEADER)?;
        let idempotency = identity_header(headers, IDEMPOTENCY_HEADER)?;
        let session_key = session.map(|value| format!("session-header:{value}"))
            .or_else(|| extract_session_key(object.get("client_metadata")))
            .or_else(|| idempotency.as_ref().map(|value| format!("idempotency:{value}")))
            .ok_or_else(|| {
            ProxyError::BadRequest(
                "batch mode requires Idempotency-Key, X-Kaiion-Session-Id, client_metadata.thread_id or client_metadata.session_id for restart-safe request identity"
                    .to_string(),
            )
        })?;

        object.insert("stream".to_string(), Value::Bool(false));
        object.remove("stream_options");

        let mut fingerprint_body = batch_body.clone();
        remove_volatile_codex_metadata(&mut fingerprint_body);
        let encoded = serde_json::to_vec(&fingerprint_body)?;
        let mut hasher = Sha256::new();
        // A persisted database can be reused after configuration changes.
        // Scope durable identities to the configured provider so an identical
        // request is never replayed from a different upstream deployment.
        hash_component(
            &mut hasher,
            b"provider",
            &canonical_provider_url(provider_namespace)?,
        );
        hash_component(&mut hasher, b"session", &session_key);
        let idempotency_hash = idempotency.map(|key| {
            hash_component(&mut hasher, b"idempotency", &key);
            let mut identity = hasher.clone();
            hash_component(&mut identity, b"identity", "idempotency-v1");
            hex_digest(identity.finalize())
        });
        hash_bytes(&mut hasher, b"request", &encoded);
        let request_hash = hex_digest(hasher.finalize());
        batch_body
            .as_object_mut()
            .expect("validated object")
            .insert("store".into(), Value::Bool(false));

        Ok(Self {
            batch_body,
            request_hash,
            model,
            async_alias,
            idempotency_hash,
        })
    }
}

pub fn canonical_provider_url(value: &str) -> Result<String, ProxyError> {
    let mut url = reqwest::Url::parse(value).map_err(|error| {
        ProxyError::BadRequest(format!("invalid upstream provider URL: {error}"))
    })?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ProxyError::BadRequest(
            "upstream provider URL cannot contain a query or fragment".to_string(),
        ));
    }
    let path = url.path().trim_end_matches('/').to_string();
    url.set_path(&path);
    Ok(url.to_string().trim_end_matches('/').to_string())
}

pub fn resolve_mode(headers: &HeaderMap, default: Mode) -> Result<Mode, ProxyError> {
    let Some(value) = headers.get(MODE_HEADER) else {
        return Ok(default);
    };
    match value.to_str().unwrap_or_default() {
        "batch" => Ok(Mode::Batch),
        "direct" => Ok(Mode::Direct),
        "auto" => Ok(Mode::Auto),
        value => Err(ProxyError::BadRequest(format!(
            "invalid {MODE_HEADER} value {value:?}; expected batch, direct or auto"
        ))),
    }
}

pub fn identity_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, ProxyError> {
    let value = optional_header(headers, name)?;
    if value
        .as_ref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 256)
    {
        return Err(ProxyError::BadRequest(format!(
            "{name} must contain 1 to 256 non-blank bytes"
        )));
    }
    Ok(value)
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
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            metadata
                .get("session_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
        })?;
    let turn = metadata
        .get("turn_id")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let mut hasher = Sha256::new();
    hasher.update(thread.as_bytes());
    hasher.update([0]);
    hasher.update(turn.as_bytes());
    Some(hex_digest(hasher.finalize()))
}

fn extract_thread_id(client_metadata: Option<&Value>) -> Option<String> {
    let metadata = client_metadata?.as_object()?;
    metadata
        .get("thread_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            metadata
                .get("session_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
        })
        .map(str::to_string)
}

fn contains_item_type(value: Option<&Value>, accepted: &[&str]) -> bool {
    match value {
        Some(Value::Array(values)) => values
            .iter()
            .any(|value| contains_item_type(Some(value), accepted)),
        Some(Value::Object(object)) => {
            object
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| accepted.contains(&kind))
                || object
                    .values()
                    .any(|value| contains_item_type(Some(value), accepted))
        }
        _ => false,
    }
}

fn contains_user_marker(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Array(values)) => values.iter().any(|value| contains_user_marker(Some(value))),
        Some(Value::Object(object)) => {
            object.get("role").and_then(Value::as_str) == Some("user")
                || object.get("type").and_then(Value::as_str) == Some("input_text")
                || object
                    .get("content")
                    .is_some_and(|content| contains_user_marker(Some(content)))
        }
        Some(Value::String(_)) => true,
        _ => false,
    }
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
            "stream": true,
            "input": [{"role": "user", "content": "hello"}],
            "client_metadata": {
                "thread_id": "thread",
                "turn_id": "turn",
                "x-codex-window-id": "window-2",
                "x-codex-turn-metadata": "volatile-2"
            }
        });
        let first = NormalizedRequest::from_body(&first, "https://provider-a.test/v1").unwrap();
        let second = NormalizedRequest::from_body(&second, "https://provider-a.test/v1").unwrap();
        assert_eq!(first.request_hash, second.request_hash);
        assert_eq!(first.batch_body["stream"], false);
        assert!(first.batch_body.get("stream_options").is_none());
    }

    #[test]
    fn preserves_turn_identity_in_fingerprint() {
        let first = json!({
            "model": "gpt-test",
            "stream": true,
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn-1"}
        });
        let second = json!({
            "model": "gpt-test",
            "stream": true,
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn-2"}
        });
        assert_ne!(
            NormalizedRequest::from_body(&first, "https://provider-a.test/v1")
                .unwrap()
                .request_hash,
            NormalizedRequest::from_body(&second, "https://provider-a.test/v1")
                .unwrap()
                .request_hash
        );
    }

    #[test]
    fn scopes_request_identity_to_the_provider() {
        let body = json!({
            "model": "gpt-test",
            "stream": true,
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn"}
        });
        assert_ne!(
            NormalizedRequest::from_body(&body, "https://provider-a.test/v1")
                .unwrap()
                .request_hash,
            NormalizedRequest::from_body(&body, "https://provider-b.test/v1")
                .unwrap()
                .request_hash
        );
    }

    #[test]
    fn equivalent_provider_urls_share_an_identity() {
        let body = json!({
            "model": "gpt-test",
            "stream": true,
            "input": [],
            "client_metadata": {"thread_id": "thread", "turn_id": "turn"}
        });
        assert_eq!(
            NormalizedRequest::from_body(&body, "https://example.com/v1")
                .unwrap()
                .request_hash,
            NormalizedRequest::from_body(&body, "https://example.com/v1/")
                .unwrap()
                .request_hash
        );
        assert_eq!(
            NormalizedRequest::from_body(&body, "HTTPS://EXAMPLE.COM:443/v1/")
                .unwrap()
                .request_hash,
            NormalizedRequest::from_body(&body, "https://example.com/v1")
                .unwrap()
                .request_hash
        );
    }

    #[test]
    fn rejects_stateful_response_semantics() {
        for field in ["store", "previous_response_id"] {
            let mut body = json!({
                "model": "gpt-test",
                "stream": true,
                "client_metadata": {"thread_id": "thread"}
            });
            body[field] = if field == "store" {
                Value::Bool(true)
            } else {
                Value::String("resp_previous".to_string())
            };
            assert!(matches!(
                NormalizedRequest::from_body(&body, "https://provider.test/v1"),
                Err(ProxyError::BadRequest(_))
            ));
        }
    }
}
