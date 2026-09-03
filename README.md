# Kaiion

Kaiion is a durable OpenAI Responses API proxy that lets Codex use either normal synchronous inference or the Batch API without modifying Codex.

## MVP behavior

- `direct` mode transparently forwards `POST /v1/responses` and its SSE response.
- `batch` mode converts the request into a one-entry Batch API job.
- Batch status is polled and the completed response is converted back into Responses SSE events.
- `response.in_progress` events keep Codex's stream alive while Batch inference is pending.
- SQLite stores upstream IDs and completed responses so an identical request can reconnect after either Codex or Kaiion restarts.
- The incoming API key, organization, and project headers are passed through to OpenAI. Credentials are never persisted; only a SHA-256 credential fingerprint is stored for request isolation.
- No pooling, automatic routing, supervisor, or webhook receiver is included.

## Run

```bash
cargo run --release -- --mode batch
```

Environment variables mirror the CLI options:

| Variable | Default |
|---|---|
| `KAIION_LISTEN` | `127.0.0.1:8787` |
| `KAIION_DATABASE_URL` | `sqlite://kaiion.db?mode=rwc` |
| `KAIION_MODE` | `batch` |
| `KAIION_OPENAI_BASE_URL` | `https://api.openai.com/v1` |
| `KAIION_POLL_INTERVAL_SECONDS` | `5` |
| `KAIION_IN_PROGRESS_INTERVAL_SECONDS` | `15` |
| `KAIION_MAX_BODY_BYTES` | `67108864` |

The default mode can be overridden per request with `X-Kaiion-Mode: batch` or `X-Kaiion-Mode: direct`. Those are the only supported execution modes.

## Codex configuration

Add a user-level provider to `~/.codex/config.toml`:

```toml
model_provider = "kaiion"

[model_providers.kaiion]
name = "Kaiion"
base_url = "http://127.0.0.1:8787/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
supports_websockets = false
stream_idle_timeout_ms = 300000
```

Then export the normal Platform API key before starting Codex:

```bash
export OPENAI_API_KEY="..."
codex
```

## Restart semantics

Batch requests are identified by the credential fingerprint and a canonical hash of the Responses request. Kaiion removes `stream`, `stream_options`, `x-codex-window-id`, and `x-codex-turn-metadata` from the identity because those fields can change when Codex reconnects. Stable session, thread, turn, prompt, and tool state remain part of the identity.

When Codex repeats an unfinished request:

1. Kaiion finds the existing SQLite job.
2. If the batch is pending, polling resumes with the API key supplied by the new connection.
3. If the result is already stored, Kaiion immediately replays it as SSE.
4. A changed prompt, tool result, thread, or turn creates a distinct job.

Because API keys are deliberately not stored, Kaiion cannot poll after its own restart until a matching client request supplies the key again. While Kaiion remains running, its in-process poller continues after a client disconnects.

There is one inherent ambiguity in a transparent HTTP proxy: an intentionally repeated, byte-equivalent inference in the same turn is indistinguishable from transport replay. Kaiion treats it as replay. A future client-generated idempotency key would remove that ambiguity.

## Tests

```bash
cargo test --all-targets
```

`tests/black_box.rs` launches:

- A fake provider implementing synchronous Responses, Files, and Batch endpoints.
- Kaiion as a separate OS process.
- A fake Codex client that sends streaming Responses requests and parses the returned SSE bytes.

The suite verifies direct passthrough, API-key passthrough, Batch conversion, `response.in_progress`, final SSE reconstruction, disconnect behavior, a full Kaiion process restart, SQLite recovery, and prevention of duplicate batch creation.

## API surface

- `POST /v1/responses`
- `POST /responses`
- `GET /healthz`

Batch mode currently requires `stream: true`, matching Codex's transport behavior.

