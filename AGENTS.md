# Repository instructions

Kaiion is a Rust Responses API proxy. Keep the externally visible contract compatible with OpenAI Responses SSE and keep Batch state transitions durable in SQLite.

Before completing a change, run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features -- --test-threads=1
```

Changes to request identity, persistence, Batch transitions, or SSE framing require black-box coverage. Restart tests must launch Kaiion as a separate process and reuse the same SQLite database.

Run black-box process tests serially so each test's fake provider, Kaiion process, and SQLite lifecycle remain deterministic:

```bash
cargo build --locked --all-features --bins
KAIION_TEST_BINARY=target/debug/kaiion cargo test --locked --test black_box --all-features -- --test-threads=1
```

Never persist API keys or raw authorization headers. Preserve direct-mode passthrough behavior. Automatic cost routing and generic durable jobs are in scope. Keep routing policy separate from execution; unknown economics must favor batch, and auto retries must reconnect to an existing batch job. Request pooling remains unimplemented and requires explicit design and partial-failure coverage.

Batch-mode request identity must remain scoped to the configured upstream provider and an explicit Idempotency-Key, X-Kaiion-Session-Id, or stable Codex session/thread identifier. The MVP assumes one Kaiion process owns a SQLite database.
