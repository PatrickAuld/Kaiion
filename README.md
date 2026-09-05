# Kaiion

**[Kaiiron website](https://patrickauld.github.io/Kaiion/) · [Getting started](https://patrickauld.github.io/Kaiion/docs/)**

Kaiion is a durable inference proxy for existing agent harnesses. It serves OpenAI Responses over SSE or JSON, turns latency-insensitive inference into Batch API jobs, and exposes detached jobs for workflows that outlive their client process.

See [the architecture evaluation and implementation roadmap](docs/long-horizon-workflows.md) for the product direction, routing model, and remaining limitations.

## Behavior

- `direct` mode transparently forwards `POST /v1/responses` and its SSE response.
- `batch` mode converts the request into a one-entry Batch API job.
- Batch status is polled and the completed response is converted back into Responses SSE events.
- `response.in_progress` events keep Codex's stream alive while Batch inference is pending.
- SQLite stores upstream IDs and terminal responses so a reissued request can reconnect after either Codex or Kaiion restarts.
- Batch jobs move through a typed durable state machine; each transition is an atomic compare-and-set operation.
- The incoming API key, organization, and project headers are passed through to OpenAI. Credentials are never persisted; only a SHA-256 credential fingerprint is stored for request isolation.
- `auto` mode selects direct inference only within explicit per-call cost and premium allowances; uncertain estimates favor batch.
- Detached jobs persist their request bodies and can resume with credentials after restart. No credentials are stored.
- Pooling, native Anthropic/Chat Completions adapters, harness supervision, and webhooks are not yet included.

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

The default mode can be overridden per request with `X-Kaiion-Mode: batch` or `X-Kaiion-Mode: direct`, or `X-Kaiion-Mode: auto`.

## Client configuration

Configure supported clients against the local proxy in one command:

```bash
kaiiron configure
```

This updates Codex, OpenCode, and Pi configuration files atomically while preserving unrelated settings. Use `--client codex`, `--client opencode`, or `--client pi` to select clients, `--home` to target another home directory, `--codex-home` or `CODEX_HOME` for a non-default Codex directory, `--model` to choose the model registered for OpenCode and Pi, and `--dry-run` to inspect the changes. OpenCode and Pi use direct mode by default. Use `--client-mode batch --session-id my-workflow` or `--client-mode auto --session-id my-workflow` to enable durable identity for clients without Codex metadata. Keep the session ID stable across retries; choose a new one for a new workflow. For exact step identity, client adapters should send an `Idempotency-Key` per inference. A static session header cannot distinguish deliberately repeated identical inference from a transport retry.

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

Batch requests are identified by the upstream provider URL, credential fingerprint, and a canonical hash of the Responses request. Kaiion removes `stream`, `stream_options`, `x-codex-window-id`, and `x-codex-turn-metadata` from the identity because those fields can change when Codex reconnects. Stable session, thread, turn, prompt, and tool state remain part of the identity. Batch and auto mode require `Idempotency-Key`, `X-Kaiion-Session-Id`, or `client_metadata.thread_id` / `client_metadata.session_id`. Explicit session headers take precedence over Codex metadata. The same explicit idempotency key with a changed payload within its session scope returns HTTP 409; a new inference needs a new key.

When Codex repeats an unfinished request:

1. Kaiion finds the existing SQLite job.
2. If the batch is pending, polling resumes with the API key supplied by the new connection.
3. If the result is already stored, Kaiion immediately replays it as SSE.
4. A changed prompt, tool result, thread, or turn creates a distinct job.

The OpenAI Batch create endpoint does not currently document a stable idempotency-key contract. Kaiion therefore optimizes for at-most-once Batch cost rather than automatic liveness after an ambiguous create. Before creation it durably records `submitting`. If the create response is lost, or Kaiion restarts from that state, the job becomes `submission_uncertain`: Kaiion searches all Batch-list pages for its durable job metadata but never creates a replacement merely because a list response is negative. The state remains explicit in SQLite and reconciliation continues when a matching client supplies credentials. An operator must inspect the provider before choosing any manual retry; the MVP has no automatic or API-triggered retry path.

API keys are not stored. Resume after restart by resending the inference, using `kaiiron jobs resume <id>`, or opting into `--resume-from-env` with `OPENAI_API_KEY` and the matching optional `OPENAI_ORG_ID` / `OPENAI_PROJECT_ID`. The last option restarts polling for matching jobs without a client reconnecting. While Kaiion remains running, its poller continues after a client disconnects. Jobs created before the durable-request migration acquire their persisted payload when the original request is first replayed.

Stock Codex currently records an interrupted in-progress turn when its process exits; it does not automatically resend that unfinished inference merely because `codex resume` starts. Kaiion recovery applies when Codex, a harness, or another client reissues the same request. Transparent automatic continuation would require a small Codex-side resend hook or an external launcher that replays the pending turn.

The MVP supports one Kaiion process per SQLite database. Multiple processes sharing one database require a cross-process polling/submission lease, which is intentionally outside this scope.

There is one inherent ambiguity in a transparent HTTP proxy: an intentionally repeated, byte-equivalent inference in the same turn is indistinguishable from transport replay. Kaiion treats it as replay. Sending a new `Idempotency-Key` for each intentional inference removes that ambiguity.

## Tests

```bash
cargo test --all-targets -- --test-threads=1
```

The process-level suite can also be run on its own:

```bash
cargo build --locked --all-features --bins
KAIION_TEST_BINARY=target/debug/kaiion cargo test --locked --test black_box --all-features -- --test-threads=1
```

The three real-client compatibility cases are enabled in CI. To run them locally, install the pinned clients and opt in:

```bash
npm install --global @openai/codex@0.104.0 opencode-ai@1.18.28 @earendil-works/pi-coding-agent@0.85.0
KAIION_TEST_BINARY=target/debug/kaiion KAIION_TEST_REAL_CLIS=1 cargo test --locked --test black_box --all-features -- --test-threads=1
```

Override client locations with `KAIION_TEST_CODEX_BINARY`, `KAIION_TEST_OPENCODE_BINARY`, and `KAIION_TEST_PI_BINARY`.

`tests/black_box.rs` launches:

- A fake provider implementing synchronous Responses, Files, and Batch endpoints.
- The real Codex, OpenCode, and Pi CLIs against isolated configurations produced by `kaiiron configure`; the provider holds each Batch in progress until the test releases it.
- Kaiion as a separate OS process.
- A fake Codex client that sends streaming Responses requests and parses the returned SSE bytes.

The suite verifies direct passthrough, API-key/organization/project passthrough, Batch wire conversion, `response.in_progress`, final SSE reconstruction, concurrent deduplication, ambiguous-create recovery with delayed list visibility, protocol-error termination, a full Kaiion process restart, and stored-result replay without further upstream calls. Provider, process, fixture, and SSE support live in focused modules under `tests/support`.

## API surface

- `POST /v1/responses`
- `POST /responses`
- `GET /healthz`
- `POST /v1/kaiion/jobs` — submit a durable batch job, return HTTP 202 and `Location`
- `GET /v1/kaiion/jobs` — list owned jobs, 100 per page; use `?after=<next_after>`
- `GET /v1/kaiion/jobs/{id}` — retrieve status and terminal response
- `POST /v1/kaiion/jobs/{id}/resume` — reattach credentials and restart polling
- `POST /v1/kaiion/route` — explain the selected route without provider calls

Batch mode is intentionally stateless from the Responses API perspective. It accepts SSE (`stream: true`) or blocking JSON (`stream: false` or omitted). It requires a non-empty `model`, durable request identity, and stateless history: no `store: true`, `previous_response_id`, `conversation`, or `background: true`. The upstream batch body always includes `store: false`. Kaiion rewrites `stream` to `false` only in the Batch input and replaces the provider response ID with a stable Kaiion response ID in emitted SSE. Stateful Responses usage will require a future durable provider/local response-ID mapping.

## Cost-aware auto mode

```bash
kaiiron --mode auto --routing-policy /absolute/path/routing-policy.json start
kaiiron jobs route --request examples/response-request.json
```

[Example policy](examples/routing-policy.json) prices are deliberately illustrative, not a model price catalog. Replace the model ID and rates with your provider's rates. Policy is read on startup; restart to reload it. Unknown model prices select batch. With no policy file, auto mode selects batch.

Direct inference must satisfy both `max_direct_cost_usd` and `max_direct_premium_usd` (estimated direct cost minus estimated batch cost, floored at zero). Set the premium to zero when avoiding latency has no economic value. These are **per-call estimates, not a cumulative workflow budget or billing guarantee**.

The estimator counts serialized instructions, input history, tools, and output schema at roughly three bytes per input token, plus framing overhead. It uses the explicit `max_output_tokens` as a conservative output allowance. Missing output limits, medium/high/xhigh/max reasoning, unknown models, and unpriced modalities/hosted tools select batch. No LLM classifier is called and no request parameters or model are downgraded. Caching, actual usage calibration, and total workflow budgets are future work.

Responses include `X-Kaiion-Mode` and `X-Kaiion-Route-Reason`. Existing batch jobs stay on batch in auto mode even after policy changes, avoiding a second charge. Explicit direct mode remains passthrough: it does not participate in durable replay. Auto-selected direct calls also retain direct transport/retry semantics; they are not durable jobs.

## Detached workflows

```bash
kaiiron jobs submit --request examples/response-request.json --idempotency-key workflow-42/step-1
kaiiron jobs list
kaiiron jobs show JOB_ID
kaiiron jobs wait JOB_ID
```

Set `OPENAI_API_KEY` before using these commands; credentials are read from the environment and sent only as HTTP headers. `jobs wait` reattaches credentials, polls until terminal, prints the stored response, and exits unsuccessfully for failed/incomplete/expired/cancelled work. Interrupting it leaves the inference running. Use `jobs --proxy-url http://host:8787 ...` for another proxy.

The detached endpoint always submits batch work, independently of the proxy default mode. `GET` is read-only; after a proxy restart, use `resume`, `wait`, or `--resume-from-env` to restart workers. Job IDs alone grant no access: retrieval, listing, and resumption require the original credential, organization, project, and configured upstream provider. A rotated API key creates a new namespace; credential rotation across existing jobs is not yet supported.

An HTTP proxy cannot checkpoint the harness's tool execution or restore its process. These commands make **inference** durable; a harness adapter must still checkpoint tool results, pending job IDs, and continuation state. Use the roadmap's adapter contract for multi-day agent runs.
