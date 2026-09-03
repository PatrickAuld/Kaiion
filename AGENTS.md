# Repository instructions

Kaiion is a Rust Responses API proxy. Keep the externally visible contract compatible with OpenAI Responses SSE and keep Batch state transitions durable in SQLite.

Before completing a change, run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Changes to request identity, persistence, Batch transitions, or SSE framing require black-box coverage. Restart tests must launch Kaiion as a separate process and reuse the same SQLite database.

Never persist API keys or raw authorization headers. Preserve direct-mode passthrough behavior. Do not introduce request pooling or an automatic routing mode without an explicit scope change.

