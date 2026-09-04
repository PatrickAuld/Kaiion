# Thermo-Nuclear Review Remediation Plan

## Status

Implemented on `main`. The findings below describe the baseline reviewed at commit `25d8c926ddaa78ab5e9bca5c2b97073efb9605c7`; they are retained as the acceptance record for the remediation.

The implementation uses typed durable states, atomic compare-and-set transitions, an explicit `submission_uncertain` recovery state, one watch-based worker registry, a provider error taxonomy, and a decomposed failure-injection harness. The full formatting, Clippy, unit, migration, and serial black-box gates pass.

## Goals

- Make the durable Batch lifecycle explicit, typed, and exhaustively handled.
- Make every state transition atomic and observable.
- Prevent automatic duplicate Batch creation after an uncertain submission.
- Delete inactive concepts rather than preserving speculative infrastructure.
- Separate provider protocol, persistence, orchestration, and SSE concerns.
- Expand failure-injection coverage without allowing the test harness to become another monolith.

## Required changes

### P0: Replace the nullable job record with a typed state model

The current `Job` combines a string status with state-dependent nullable columns. Invalid states remain representable, and orchestration reconstructs invariants through sequential string comparisons.

Introduce explicit domain types:

```rust
enum JobState {
    Queued,
    Uploaded { input_file_id: FileId },
    Submitting {
        input_file_id: FileId,
        started_at: Timestamp,
    },
    Submitted { batch_id: BatchId },
    Terminal(StoredOutcome),
}

enum StoredOutcome {
    Completed(Value),
    Failed(Value),
    Incomplete(Value),
    Expired(Value),
    Cancelled(Value),
}
```

Required implementation properties:

- Database rows are decoded into `JobState` at the persistence boundary.
- Impossible column combinations fail decoding as a typed persistence error.
- Runtime orchestration does not compare raw status strings.
- `drive_job` becomes one exhaustive state dispatch rather than cascading `if` blocks and reloads.
- State-dependent identifiers are non-optional after decoding.
- Status serialization remains private to the database module.

### P0: Make transitions atomic and conflict-aware

The guarded update methods currently report success even when no row changed.

Required implementation properties:

- Transitions use compare-and-set semantics and return the resulting typed state, preferably through `UPDATE ... RETURNING`.
- Zero changed rows return a typed transition conflict.
- A worker cannot report terminal completion unless the terminal outcome was durably stored.
- Result persistence and terminal status change occur in the same transaction.
- Transition conflicts cause re-read/reconciliation, not silent worker exit.
- Database constraints reject invalid terminal and identifier combinations where SQLite can express them cleanly.

### P0: Model uncertain Batch submission explicitly

A negative Batch-list lookup is not proof that a previous create request was never accepted. Eventual list visibility plus a lost create response can currently produce a second paid Batch.

Required behavior:

1. Determine whether the provider offers a stable idempotency mechanism for Batch creation.
2. If supported, derive the idempotency key from the durable `JobId` and eliminate metadata-list lookup as the uniqueness mechanism.
3. If unsupported, distinguish fresh submission ownership from recovered submission uncertainty.
4. Default to at-most-once cost safety: an uncertain recovered submission must not automatically create another Batch based on one negative list result.
5. Expose unresolved submission state explicitly for continued reconciliation or operator-authorized retry.
6. Document the at-most-once versus liveness tradeoff.

### P1: Delete inactive delivery and retry scaffolding

Remove concepts that do not currently change behavior:

- `delivered_at` and its write paths
- persisted `session_key` and its index, unless a concrete read-side invariant requires them
- `attempt` until explicit retry behavior exists
- unused SSE assembly paths

Stable session metadata should be validated while constructing a non-optional normalized Batch request. If retries are later introduced, add a real `AttemptId` and explicit retry transition then.

### P1: Consolidate runtime coordination

The current poller set, notifier map, database polling loop, and SSE heartbeat loop split ownership across multiple mechanisms. Notifier entries are not evicted.

Replace them with one worker registry keyed by `JobId`:

- Each worker owns a `watch::Sender<JobState>`.
- Streams subscribe through `watch::Receiver<JobState>`.
- The registry never starts a worker for a terminal job.
- Registry entries are removed when workers terminate.
- Existing receivers retain the terminal value after eviction.
- Database state remains authoritative across process restart.

### P1: Separate provider transport errors from protocol errors

Successful HTTP responses containing malformed or incompatible JSON must not become indefinitely retryable transport errors.

Introduce a provider error taxonomy:

- retryable transport failure
- retryable HTTP status
- permanent HTTP status
- non-retryable provider protocol violation
- retryable output-file visibility delay

Read response bytes first, then deserialize separately so JSON/schema failures are classified as provider protocol errors and durably terminate the job.

### P1: Define the supported Responses request contract

Batch mode replaces the provider response ID with a Kaiion ID. This is incompatible with stateful Responses usage unless IDs are mapped and rewritten.

For the MVP:

- Reject `store: true`.
- Reject `previous_response_id`.
- Continue requiring `stream: true`.
- Return precise invalid-request errors for unsupported semantics.
- Document the supported Codex-oriented request subset.

A later version may persist provider/local response-ID mappings instead.

### P1: Decompose the black-box harness

`tests/black_box.rs` is approximately 938 lines and currently owns provider behavior, process control, SSE parsing, fixtures, and scenarios.

Split it into:

- `tests/support/provider.rs`
- `tests/support/process.rs`
- `tests/support/sse.rs`
- `tests/support/fixtures.rs`
- focused scenario files or a concise `black_box.rs`

The fake provider should support scripted responses, delayed visibility, injected disconnects, malformed payloads, pagination, and endpoint call assertions without adding scenario-specific branches throughout handlers.

## Required test coverage

### Durability and uncertainty

- Crash after file upload but before recording the file ID.
- Crash after recording the file ID but before Batch creation.
- Provider accepts Batch creation, connection fails before the response arrives, and Batch-list visibility is delayed.
- Crash after retrieving output but before persisting it.
- Crash after persisting the terminal result but before delivery.
- Restart from every persisted non-terminal state.
- Migration of a populated version-1 database.

### Concurrency

- Simultaneous identical client requests create at most one upstream Batch.
- Concurrent transition conflicts reconcile without losing the worker.
- Worker registry entries are evicted after terminal completion.
- Notification timing cannot delay delivery until the next heartbeat.

### Provider behavior

- Batch statuses: completed, failed, expired, and cancelled.
- Responses statuses: completed, failed, and incomplete.
- Completion with only an error file.
- Output/error files temporarily return 404 or 409.
- Retryable 429 and 5xx responses.
- Permanent 4xx responses.
- Malformed successful JSON and missing required fields.
- Wrong or missing `custom_id`.
- Batch-list pagination beyond 100 entries.

### API contract

- Missing authorization.
- Invalid mode.
- `stream: false` in Batch mode.
- Missing stable session metadata.
- `store: true` and `previous_response_id`.
- Equivalent normalized provider URLs.
- Exact direct-mode body, status, and header passthrough.
- Integration through the actual Codex SSE parser, including `response.incomplete`.

## Success criteria

The remediation is complete only when all of the following are true:

### State and persistence

- No production orchestration code compares job-state strings.
- Every loaded job is represented by a valid `JobState`.
- No state-dependent identifier is optional within its corresponding state.
- Every transition detects compare-and-set conflicts.
- Terminal status and terminal payload are persisted atomically.
- A failed transition cannot cause the only worker to exit while the job remains non-terminal.

### Cost safety and recovery

- The ambiguous-create failure-injection test proves that recovery never automatically creates a second paid Batch.
- Restart behavior is deterministic for every durable state.
- Any liveness tradeoff caused by at-most-once submission is surfaced explicitly rather than hidden by retries.
- Stored terminal outcomes replay without upstream calls.

### Runtime behavior

- One active worker exists per non-terminal `JobId` within a process.
- Terminal jobs do not start workers.
- Worker-registry size returns to baseline after all streams and jobs terminate.
- Provider protocol violations reach a durable terminal state instead of retrying forever.
- SSE sequence numbers and response IDs remain stable and monotonic across each connection.

### Maintainability

- Dead delivery/retry fields and paths are removed.
- Provider, persistence, driver, coordination, and SSE layers have explicit ownership.
- `proxy.rs` is reduced to HTTP routing and stream orchestration rather than provider-state business logic.
- No source or test file exceeds 1,000 lines.
- The black-box harness is decomposed before adding the expanded matrix.
- The resulting design reduces the number of states and optional concepts a reader must hold simultaneously.

### Verification

These checks pass on every pull request and on `main`:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --bins --all-features
cargo test --test black_box --all-features -- --test-threads=1
```

The final implementation receives a fresh Thermo-Nuclear review with no blocker or high-severity structural findings.
