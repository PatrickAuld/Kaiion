use bytes::Bytes;
use serde_json::{Value, json};

use crate::error::ProxyError;

pub fn created_event(response_id: &str, model: &str, sequence_number: u64) -> Bytes {
    event(
        "response.created",
        &json!({
            "type": "response.created",
            "sequence_number": sequence_number,
            "response": response_stub(response_id, model, "in_progress")
        }),
    )
}

pub fn in_progress_event(response_id: &str, model: &str, sequence_number: u64) -> Bytes {
    event(
        "response.in_progress",
        &json!({
            "type": "response.in_progress",
            "sequence_number": sequence_number,
            "response": response_stub(response_id, model, "in_progress")
        }),
    )
}

pub fn completion_events(
    response: &Value,
    response_id: &str,
    first_sequence_number: u64,
) -> Result<Vec<Bytes>, ProxyError> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| ProxyError::Internal("batch response is missing output".to_string()))?;

    let mut completed_response = response.clone();
    let object = completed_response
        .as_object_mut()
        .ok_or_else(|| ProxyError::Internal("batch response is not a JSON object".to_string()))?;
    object.insert("id".to_string(), Value::String(response_id.to_string()));

    let mut sequence = first_sequence_number;
    let mut events = Vec::new();

    for (output_index, item) in output.iter().enumerate() {
        events.push(event(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "sequence_number": sequence,
                "output_index": output_index,
                "item": item
            }),
        ));
        sequence += 1;

        if item.get("type").and_then(Value::as_str) == Some("message")
            && let (Some(item_id), Some(content)) = (
                item.get("id").and_then(Value::as_str),
                item.get("content").and_then(Value::as_array),
            )
        {
            for (content_index, part) in content.iter().enumerate() {
                if part.get("type").and_then(Value::as_str) != Some("output_text") {
                    continue;
                }
                let text = part.get("text").and_then(Value::as_str).unwrap_or("");
                events.push(event(
                    "response.content_part.added",
                    &json!({
                        "type": "response.content_part.added",
                        "sequence_number": sequence,
                        "item_id": item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "part": {"type": "output_text", "text": "", "annotations": []}
                    }),
                ));
                sequence += 1;
                if !text.is_empty() {
                    events.push(event(
                        "response.output_text.delta",
                        &json!({
                            "type": "response.output_text.delta",
                            "sequence_number": sequence,
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": content_index,
                            "delta": text,
                            "logprobs": []
                        }),
                    ));
                    sequence += 1;
                }
                events.push(event(
                    "response.output_text.done",
                    &json!({
                        "type": "response.output_text.done",
                        "sequence_number": sequence,
                        "item_id": item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "text": text,
                        "logprobs": []
                    }),
                ));
                sequence += 1;
                events.push(event(
                    "response.content_part.done",
                    &json!({
                        "type": "response.content_part.done",
                        "sequence_number": sequence,
                        "item_id": item_id,
                        "output_index": output_index,
                        "content_index": content_index,
                        "part": part
                    }),
                ));
                sequence += 1;
            }
        }

        events.push(event(
            "response.output_item.done",
            &json!({
                "type": "response.output_item.done",
                "sequence_number": sequence,
                "output_index": output_index,
                "item": item
            }),
        ));
        sequence += 1;
    }

    events.push(event(
        "response.completed",
        &json!({
            "type": "response.completed",
            "sequence_number": sequence,
            "response": completed_response
        }),
    ));
    Ok(events)
}

pub fn failed_event(response_id: &str, model: &str, sequence_number: u64, message: &str) -> Bytes {
    event(
        "response.failed",
        &json!({
            "type": "response.failed",
            "sequence_number": sequence_number,
            "response": {
                "id": response_id,
                "object": "response",
                "created_at": 0,
                "status": "failed",
                "model": model,
                "output": [],
                "error": {
                    "code": "kaiion_batch_failed",
                    "message": message,
                    "type": "server_error"
                }
            }
        }),
    )
}

/// Preserves an upstream terminal Responses object when Batch output contains
/// one, including the distinction between `failed` and `incomplete`.
pub fn terminal_error_event(
    response_id: &str,
    model: &str,
    sequence_number: u64,
    error_json: &str,
) -> Bytes {
    let Ok(mut response) = serde_json::from_str::<Value>(error_json) else {
        return failed_event(response_id, model, sequence_number, error_json);
    };
    let Some(object) = response.as_object_mut() else {
        return failed_event(response_id, model, sequence_number, error_json);
    };
    let Some(status) = object.get("status").and_then(Value::as_str) else {
        return failed_event(response_id, model, sequence_number, error_json);
    };
    let kind = match status {
        "failed" => "response.failed",
        "incomplete" => "response.incomplete",
        _ => return failed_event(response_id, model, sequence_number, error_json),
    };
    object.insert("id".to_string(), Value::String(response_id.to_string()));
    object
        .entry("object".to_string())
        .or_insert_with(|| Value::String("response".to_string()));
    object
        .entry("model".to_string())
        .or_insert_with(|| Value::String(model.to_string()));
    object
        .entry("output".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    event(
        kind,
        &json!({
            "type": kind,
            "sequence_number": sequence_number,
            "response": response,
        }),
    )
}

fn response_stub(response_id: &str, model: &str, status: &str) -> Value {
    json!({
        "id": response_id,
        "object": "response",
        "created_at": 0,
        "status": status,
        "model": model,
        "output": []
    })
}

fn event(kind: &str, data: &Value) -> Bytes {
    Bytes::from(format!(
        "event: {kind}\ndata: {}\n\n",
        serde_json::to_string(data).expect("serializing a JSON value cannot fail")
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn converts_a_complete_response_to_codex_compatible_events() {
        let response = json!({
            "id": "resp_1",
            "object": "response",
            "created_at": 1,
            "status": "completed",
            "model": "gpt-test",
            "output": [{
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "done", "annotations": []}]
            }],
            "usage": {
                "input_tokens": 1,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 1,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 2
            }
        });
        let encoded = completion_events(&response, "resp_kaiion_test", 2)
            .unwrap()
            .into_iter()
            .map(|event| String::from_utf8(event.to_vec()).unwrap())
            .collect::<String>();
        assert!(encoded.contains("event: response.output_item.done"));
        assert!(encoded.contains("event: response.output_item.added"));
        assert!(encoded.contains("event: response.output_text.delta"));
        assert!(encoded.contains("event: response.output_text.done"));
        assert!(encoded.contains("event: response.content_part.added"));
        assert!(encoded.contains("event: response.content_part.done"));
        assert!(encoded.contains("event: response.completed"));
        assert!(encoded.contains("\"id\":\"resp_kaiion_test\""));
        assert!(encoded.contains("\"text\":\"done\""));
    }
}
