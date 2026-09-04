use std::{net::SocketAddr, pin::Pin, time::Duration};

use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::Value;

use super::provider::FakeProvider;

pub struct FakeCodex {
    client: reqwest::Client,
    address: SocketAddr,
}

pub struct CodexStream {
    pub status: StatusCode,
    pub headers: HeaderMap,
    bytes: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct SseEvent {
    pub kind: String,
    pub data: Value,
}

impl FakeCodex {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            client: reqwest::Client::new(),
            address,
        }
    }

    pub async fn send(&self, request: &Value) -> CodexStream {
        self.send_with_headers(request, "test-key", None, None)
            .await
    }

    pub async fn send_with_headers(
        &self,
        request: &Value,
        api_key: &str,
        organization: Option<&str>,
        project: Option<&str>,
    ) -> CodexStream {
        let mut outbound = self
            .client
            .post(format!("http://{}/v1/responses", self.address))
            .bearer_auth(api_key);
        if let Some(organization) = organization {
            outbound = outbound.header("openai-organization", organization);
        }
        if let Some(project) = project {
            outbound = outbound.header("openai-project", project);
        }
        let response = outbound.json(request).send().await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        CodexStream {
            status,
            headers,
            bytes: Box::pin(response.bytes_stream()),
            buffer: Vec::new(),
        }
    }
}

impl CodexStream {
    pub async fn all_bytes(mut self) -> Vec<u8> {
        let mut bytes = std::mem::take(&mut self.buffer);
        while let Some(next) = self.bytes.next().await {
            bytes.extend_from_slice(&next.expect("SSE transport failed"));
        }
        bytes
    }

    pub async fn next_event(&mut self) -> Option<SseEvent> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(event) = take_sse_event(&mut self.buffer) {
                    return Some(event);
                }
                match self.bytes.next().await {
                    Some(Ok(bytes)) => self.buffer.extend_from_slice(&bytes),
                    Some(Err(error)) => panic!("SSE transport failed: {error}"),
                    None => return take_sse_event(&mut self.buffer),
                }
            }
        })
        .await
        .expect("SSE event timed out")
    }

    pub async fn through_terminal(&mut self) -> Vec<SseEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.next_event().await {
            let terminal = matches!(
                event.kind.as_str(),
                "response.completed" | "response.failed" | "response.incomplete"
            );
            events.push(event);
            if terminal {
                break;
            }
        }
        events
    }
}

fn take_sse_event(buffer: &mut Vec<u8>) -> Option<SseEvent> {
    let boundary = buffer.windows(2).position(|window| window == b"\n\n")?;
    let frame = buffer.drain(..boundary + 2).collect::<Vec<_>>();
    let frame = String::from_utf8(frame).expect("fake provider emitted invalid UTF-8");
    let kind = frame
        .lines()
        .find_map(|line| line.strip_prefix("event: "))?
        .to_string();
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect::<Vec<_>>()
        .join("\n");
    Some(SseEvent {
        kind,
        data: serde_json::from_str(&data).expect("fake provider emitted invalid SSE JSON"),
    })
}

pub fn events_contain(events: &[SseEvent], needle: &str) -> bool {
    events
        .iter()
        .any(|event| event.data.to_string().contains(needle))
}

pub async fn wait_for_batch(provider: &FakeProvider) {
    wait_for_batch_count(provider, 1).await;
}

pub async fn wait_for_batch_count(provider: &FakeProvider, expected: usize) {
    for _ in 0..400 {
        if provider.batch_creations() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Kaiion did not create {expected} batches");
}

pub async fn wait_for_provider_call(provider: &FakeProvider, path: &str) {
    for _ in 0..400 {
        if provider.calls().await.iter().any(|call| call.path == path) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Kaiion did not call provider endpoint {path}");
}

pub async fn expect_batch_lifecycle_start(stream: &mut CodexStream) -> String {
    let created = stream.next_event().await.unwrap();
    assert_eq!(created.kind, "response.created");
    assert_eq!(created.data["sequence_number"], 0);
    assert_eq!(created.data["response"]["status"], "in_progress");
    let response_id = created.data["response"]["id"].as_str().unwrap().to_string();
    let progress = stream.next_event().await.unwrap();
    assert_eq!(progress.kind, "response.in_progress");
    assert_eq!(progress.data["sequence_number"], 1);
    assert_eq!(
        progress.data["response"]["id"].as_str(),
        Some(response_id.as_str())
    );
    response_id
}
