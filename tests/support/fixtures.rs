use serde_json::{Value, json};

pub fn codex_request(window_id: &str) -> Value {
    json!({
        "model": "gpt-test",
        "instructions": "Return a result",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }],
        "tools": [],
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "store": false,
        "stream": true,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": "session-1",
        "client_metadata": {
            "session_id": "session-1",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "x-codex-window-id": window_id,
            "x-codex-turn-metadata": format!("metadata-{window_id}")
        }
    })
}
