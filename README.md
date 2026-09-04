# Kaiion

Kaiion is a durable OpenAI Responses API proxy that lets Codex use either normal synchronous inference or the Batch API without modifying Codex.

## MVP behavior

- `direct` mode transparently forwards `POST /v1/responses` and its SSE response.
- `batch` mode converts the request into a one-entry Batch API job.
- Batch status is polled and the completed response is converted back into Responses SSE events.
- `response.in_progress` events keep Codex's stream alive while Batch inference is pending.
- SQLite stores upstream IDs and terminal responses so a reissued request can reconnect after either Codex or Kaiion restarts.
- Batch jobs move through a typed durable state machine; each transition is an atomic compare-and-set operation.
- The incoming API key, organization, and project headers are passed through to OpenAI. Credentials are never persisted; only a SHA-256 credential fingerprint is stored for request isolation.
- No pooling, automatic routing, supervisor, or webhook receiver is included.

## Run

```bash
cargo run --release -- --mode batch
```

For a managed local service, install the binary and use its lifecycle commands:

```bash
cargo install --path . --locked
kaiiron start
kaiiron status
kaiiron logs --lines 100
kaiiron restart
kaiiron stop
```

`kaiion` and `kaiiron` are equivalent binary names. The service stores its effective configuration under `$XDG_CONFIG_HOME/kaiion` or `~/.config/kaiion`, and its database, PID, readiness marker, and log under `$XDG_STATE_HOME/kaiion` or `~/.local/state/kaiion`. Override those locations with `--config-dir`, `--state-dir`, `--pid-file`, and `--log-file`. `start` waits for a child-owned readiness marker and `/healthz` before returning. `stop` sends SIGTERM, escalating to SIGKILL if necessary. `restart` reuses the saved configuration and working directory, so it is not affected by a later shell directory or environment change.

Environment variables mirror the CLI options:

| Variable | Default |
|---|---|
| `KAIION_LISTEN` | `127.0.0.1:8787` |
| `KAIION_DATABASE_URL` | `sqlite://~/.local/state/kaiion/kaiion.db?mode=rwc` |
| `KAIION_MODE` | `batch` |
| `KAIION_OPENAI_BASE_URL` | `https://api.openai.com/v1` |
| `KAIION_POLL_INTERVAL_SECONDS` | `5` |
| `KAIION_IN_PROGRESS_INTERVAL_SECONDS` | `15` |
| `KAIION_MAX_BODY_BYTES` | `67108864` |

The default mode can be overridden per request with `X-Kaiion-Mode: batch` or `X-Kaiion-Mode: direct`. Those are the only supported execution modes.

## Client configuration

Configure supported clients against the local proxy in one command:

```bash
kaiiron configure
```

This updates Codex, OpenCode, and Pi configuration files atomically while preserving unrelated settings. Use `--client codex`, `--client opencode`, or `--client pi` to select clients, `--home` to target another home directory, `--codex-home` or `CODEX_HOME` for a non-default Codex directory, `--model` to choose the model registered for OpenCode and Pi, and `--dry-run` to inspect the changes. OpenCode and Pi use direct mode by default because Kaiion's Batch contract requires stable Codex session metadata; pass `--batch` only when the client supplies that metadata.

Claude Code is intentionally rejected by `configure --client claude`: Claude Code speaks Anthropic Messages, while this release exposes OpenAI Responses. Pointing `ANTHROPIC_BASE_URL` at Kaiion without a protocol adapter would fail at runtime.

The configure command uses the local listener as its default endpoint (`http://127.0.0.1:8787/v1`). Override it with `--proxy-url`.

### Codex configuration

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

Batch requests are identified by the upstream provider URL, credential fingerprint, and a canonical hash of the Responses request. Kaiion removes `stream`, `stream_options`, `x-codex-window-id`, and `x-codex-turn-metadata` from the identity because those fields can change when Codex reconnects. Stable session, thread, turn, prompt, and tool state remain part of the identity. Batch mode rejects requests without `client_metadata.thread_id` or `client_metadata.session_id`; that stable identity is required to make replay safe.

When Codex repeats an unfinished request:

1. Kaiion finds the existing SQLite job.
2. If the batch is pending, polling resumes with the API key supplied by the new connection.
3. If the result is already stored, Kaiion immediately replays it as SSE.
4. A changed prompt, tool result, thread, or turn creates a distinct job.

The OpenAI Batch create endpoint does not currently document a stable idempotency-key contract. Kaiion therefore optimizes for at-most-once Batch cost rather than automatic liveness after an ambiguous create. Before creation it durably records `submitting`. If the create response is lost, or Kaiion restarts from that state, the job becomes `submission_uncertain`: Kaiion searches all Batch-list pages for its durable job metadata but never creates a replacement merely because a list response is negative. The state remains explicit in SQLite and reconciliation continues when a matching client supplies credentials. An operator must inspect the provider before choosing any manual retry; the MVP has no automatic or API-triggered retry path.

Because API keys are deliberately not stored, Kaiion cannot poll after its own restart until a matching client request supplies the key again. While Kaiion remains running, its in-process poller continues after a client disconnects.

Stock Codex currently records an interrupted in-progress turn when its process exits; it does not automatically resend that unfinished inference merely because `codex resume` starts. Kaiion recovery applies when Codex, a harness, or another client reissues the same request. Transparent automatic continuation would require a small Codex-side resend hook or an external launcher that replays the pending turn.

The MVP supports one Kaiion process per SQLite database. Multiple processes sharing one database require a cross-process polling/submission lease, which is intentionally outside this scope.

There is one inherent ambiguity in a transparent HTTP proxy: an intentionally repeated, byte-equivalent inference in the same turn is indistinguishable from transport replay. Kaiion treats it as replay. A future client-generated idempotency key would remove that ambiguity.

## Tests

```bash
cargo test --all-targets -- --test-threads=1
```

The process-level suite can also be run on its own:

```bash
cargo build --locked --all-features --bins
KAIION_TEST_BINARY=target/debug/kaiion cargo test --locked --test black_box --all-features -- --test-threads=1
```

`tests/black_box.rs` launches:

- A fake provider implementing synchronous Responses, Files, and Batch endpoints.
- Kaiion as a separate OS process.
- A fake Codex client that sends streaming Responses requests and parses the returned SSE bytes.

The suite verifies direct passthrough, API-key/organization/project passthrough, Batch wire conversion, `response.in_progress`, final SSE reconstruction, concurrent deduplication, ambiguous-create recovery with delayed list visibility, protocol-error termination, a full Kaiion process restart, and stored-result replay without further upstream calls. Provider, process, fixture, and SSE support live in focused modules under `tests/support`.

## API surface

- `POST /v1/responses`
- `POST /responses`
- `GET /healthz`

Batch mode is intentionally stateless from the Responses API perspective. It requires `stream: true`, `store: false`, no `previous_response_id`, a non-empty `model`, and stable `client_metadata.thread_id` or `client_metadata.session_id`. Kaiion rewrites `stream` to `false` only in the Batch input and replaces the provider response ID with a stable Kaiion response ID in emitted SSE. Stateful Responses usage will require a future durable provider/local response-ID mapping.
