use bytes::Bytes;
use serde_json::{Value, json};

use crate::error::ProxyError;

pub fn in_progress_event(job_id: &str, model: &str) -> Bytes {
    event(
        "response.in_progress",
        &json!({
            "type": "response.in_progress",
            "sequence_number": 0,
            "response": {
                "id": format!("resp_kaiion_{job_id}"),
                "object": "response",
                "created_at": 0,
                "status": "in_progress",
                "model": model,
                "output": []
            }
        }),
    )
}

pub fn completed_events(response: &Value) -> Result<Vec<Bytes>, ProxyError> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| ProxyError::Internal("batch response is missing output".to_string()))?;

    let mut created_response = response.clone();
    if let Some(object) = created_response.as_object_mut() {
        object.insert("status".to_string(), Value::String("in_progress".to_string()));
        object.insert("output".to_string(), Value::Array(Vec::new()));
        object.remove("usage");
    }

    let mut sequence = 0_u64;
    let mut events = vec![event(
        "response.created",
        &json!({
            "type": "response.created",
            "sequence_number": sequence,
            "response": created_response
        }),
    )];

    for (output_index, item) in output.iter().enumerate() {
        sequence += 1;
        events.push(event(
            "response.output_item.done",
            &json!({
                "type": "response.output_item.done",
                "sequence_number": sequence,
                "output_index": output_index,
                "item": item
            }),
        ));
    }

    sequence += 1;
    events.push(event(
        "response.completed",
        &json!({
            "type": "response.completed",
            "sequence_number": sequence,
            "response": response
        }),
    ));
    Ok(events)
}

pub fn failed_event(job_id: &str, model: &str, message: &str) -> Bytes {
    event(
        "response.failed",
        &json!({
            "type": "response.failed",
            "sequence_number": 0,
            "response": {
                "id": format!("resp_kaiion_{job_id}"),
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
        let encoded = completed_events(&response)
            .unwrap()
            .into_iter()
            .map(|event| String::from_utf8(event.to_vec()).unwrap())
            .collect::<String>();
        assert!(encoded.contains("event: response.created"));
        assert!(encoded.contains("event: response.output_item.done"));
        assert!(encoded.contains("event: response.completed"));
        assert!(encoded.contains("\"text\":\"done\""));
    }
}
