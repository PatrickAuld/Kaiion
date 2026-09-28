# Live inference scheduling plan

Kaiion already routes Responses requests to an immediate live call or to a durable Batch API job. Scheduling adds a third execution path: Kaiion accepts a request now, waits for policy-permitted capacity, then calls the provider's normal live inference endpoint. It is a live request with delayed dispatch; it does not become a Batch job unless routing policy explicitly selects batch.

The goal is to use provider capacity evenly through each configured quota window, preserve useful prompt-cache opportunities, favor sessions where a person is actively waiting, and move wait-tolerant work out of the live lane. Scheduling must be inspectable and bounded: a priority can order eligible work, but it cannot create quota or promise a provider cache hit.

## First implementation slice and limits

The initial scheduling slice is opt-in and process-local. Its queue and quota accounting live in memory: a restart loses waiting entries and the local quota ledger, and separate Kaiion processes do not coordinate. It provides bounded waits and priority decisions, and reconciles completed token usage into the in-memory rolling token ledger, but does not make scheduled requests durable, restore the ledger after restart, or turn a queued request into a replayable job. The scheduler waits before returning HTTP headers and does not yet send a synthetic SSE progress heartbeat during the wait, so clients with short request timeouts can disconnect. Keep provider headroom for usage outside this Kaiion process.

The policy file is selected with `--scheduling-policy PATH` or `KAIION_SCHEDULING_POLICY`. The current schema has an `enabled` flag and a `limits` array. Each limit may select a `provider` and/or `model`, requires `window_seconds`, and may set `max_requests`, `max_rps`, `max_input_tokens`, `max_output_tokens`, and `max_total_tokens`. `provider` matches the canonical configured upstream base URL; `model` matches the resolved provider model without the `async-` alias. An omitted selector applies to all values for that dimension. Matching limits compose. By default a provider/model quota is shared across credentials; set `auth_scoped: true` on a limit to scope that quota to an auth fingerprint. Session priority is always scoped by provider, credential, and stable session ID. `max_requests` is a sliding-window count and can allow bursts; `max_rps` paces requests evenly. Token caps also pace dispatch and completed observed usage reconciles the in-memory rolling token ledger. Additional controls include `max_wait_ms` (30,000 by default), `max_retries` (2 by default), `queue_coalesce_ms` (2 by default), `async_max_wait_ms` for wait-tolerant aliases, cache/prompt/idle windows, the fixed `async-` model prefix, complexity heuristics, and a high-priority threshold. Requests without `max_output_tokens` reserve a conservative 16,384 output-token estimate. See [the example policy](../examples/scheduling-policy.json).

The priority settings are additive inputs to the scheduler score. Their defaults are configurable:

| Setting | Default | Effect |
|---|---:|---|
| `cache_affinity_window_ms` / `cache_affinity_boost` | 60,000 ms / 40 | Boost a session after its previous response reported a cache read |
| `recent_prompt_window_ms` / `recent_prompt_boost` | 10,000 ms / 30 | Boost a new user prompt that quickly follows recent session activity |
| `tool_output_penalty` | 15 | Downweight a request whose input is tool output |
| `idle_decay_ms` / `idle_decay_penalty_per_interval` | 120,000 ms / 5 | Reduce priority as the session becomes idle |
| `aging_interval_ms` / `queue_aging_boost_per_interval` | 1,000 ms / 5 | Raise priority of requests as they wait |
| `async_alias_penalty` | 20 | Lower priority for the wait-tolerant alias |
| `high_priority_threshold` | 30 | Threshold for treating the resulting score as high priority |
| `batch_on_cache_miss` | `true` | Allows auto mode to recommend Batch for a known cache miss below the threshold |
| `complexity` | see example | Built-in heuristic thresholds and score adjustments; custom Rust classifiers use the same trait |

The `complexity` heuristic defaults to low/medium/high input bands (2,048 and 16,384 estimated input tokens) and a high-tool-count threshold of four. Its default score adjustments are +10 for low, 0 for medium, and -15 for high complexity. A high-priority label is not a quota exemption.

The sections below describe the intended architecture and staged work. Items named as acceptance gates are future work unless explicitly listed in the first-slice policy documentation and example.

## Execution paths

| Path | Provider call | Use |
|---|---|---|
| Direct | Live endpoint immediately | Existing compatibility mode when scheduling is not enabled |
| Scheduled live | Live endpoint after Kaiion admission | Live requests that can wait for fair capacity, cache affinity, or retry timing |
| Batch | Provider Batch API | Explicit batch requests and eligible low-priority work selected by auto routing |

An explicit batch request remains batch work. A scheduling policy enables the scheduled-live queue for live/direct requests. Existing clients continue to use the Responses API; scheduling is server-side and does not require a client scheduler.

## Request and dispatch flow

1. **Validate and normalize.** Validate the Responses body, provider/model, auth context, and durable request identity. Resolve any virtual model alias to its provider model while retaining the requested alias as a routing hint.
2. **Capture scheduling inputs.** Estimate input and output tokens, record the provider/model quota scopes, associate the request with its session, identify whether it contains a new user turn or only tool output, and inspect available prior-turn/cache usage evidence.
3. **Choose a lane and priority.** Explicit batch stays batch. Other requests receive a bounded effective priority assembled from policy rules. Auto mode may select Batch for work that is wait-tolerant, low priority, or unlikely to reach a useful cache window in time, when the request is batch-compatible.
4. **Reserve quota and enqueue.** Reserve estimated demand against every applicable provider and model limit. If capacity is not available now, compute the earliest eligible dispatch time and enqueue by priority with aging and per-session fairness. Priority never bypasses quota.
5. **Dispatch live.** Send the request to the live Responses API when its reservation is eligible. The only body change permitted by the model-alias contract is replacing the virtual `async-` model with its resolved provider model; the scheduler must not change reasoning settings, tools, or output limit.
6. **Reconcile and complete.** Record provider usage and rate-limit/reset headers, settle or release the reservation, and return the existing Responses JSON or SSE shape. Durable reconciliation and a scheduling-specific SSE in-progress heartbeat are future work; the initial in-memory slice must not claim restart recovery for queued live requests.

The durable request identity should continue to include the configured upstream provider, credential fingerprint, and an explicit idempotency key or stable session/thread identity. A future durable scheduler should attach replays of a pending scheduled request to the existing entry; they must not reserve quota or send a second live call. The first in-memory slice cannot promise this across process restarts.

## Composing rules

Scheduling has two separate decisions: **which lane** a request uses and **when it may consume capacity**. First determine eligibility and hard quota constraints. Then use priority to choose among requests that can be admitted. Compose the requested rules in this order:

1. **Hard provider/model limits.** Enforce every matching rolling request-count, paced request-rate, and input/output/total-token limit, both model-specific and provider-wide. The tightest applicable constraint wins. `max_requests` constrains the number of requests in a rolling window; `max_rps` spaces eligible dispatches evenly. No priority rule can exceed a limit.
2. **Lane and deadline policy.** Respect explicit `batch` selection. A request that cannot be admitted before its maximum useful wait may be sent to batch only when auto routing, request compatibility, and policy allow it; otherwise keep it queued until its configured deadline, then return a clear overload/timeout result.
3. **Session cache affinity.** Give an eligible continuation a time-bounded boost when it follows a prior session request quickly enough to plausibly reuse a provider cache. Preserve the reusable prefix where the request makes that detectable. Use observed cached-token usage to improve future estimates. The boost expires with the configured cache window; it does not claim to pin provider cache state.
4. **Current user activity.** Raise session priority when a new user turn (not a tool-call result) arrives soon after the prior answer, since this is evidence that a person is waiting. Let the boost decay as the session becomes idle; later turns can move behind active sessions or into the batch lane under auto policy.
5. **Expected complexity.** A classifier can contribute a bounded complexity band or score. Policy maps that output to a priority adjustment. The classifier is a hint; it cannot bypass quota or turn a request into a different model. Its failure must fall back to a configured neutral/conservative value.
6. **Fairness and aging.** Use stable FIFO ordering within equal priority and age old queued work so low-priority sessions do not starve. Apply per-session limits so one workflow cannot occupy the entire queue.

Policy should expose both the effective priority and the reason for it in route/scheduler diagnostics. Keep individual rule contributions visible so operators can explain why a request waited or moved to batch.

## Target quota accounting and reconciliation

Quota limits are scoped to the provider and may also be scoped to a model. A request consumes all matching scopes. `max_requests` limits the rolling count in each configured window and may permit bursts. `max_rps` and token caps pace requests evenly to spread capacity over time. By default, provider/model counters aggregate calls across credentials; an `auth_scoped` limit separates one credential's quota. Avoid assuming that per-model limits are independent of provider-wide limits.

Before dispatch, reserve estimated demand for every relevant scope. The estimate should include serialized instructions, history, tools, schemas, modality allowance where supported, and the requested output cap. If usage cannot be estimated confidently, use the policy's conservative estimate or refuse live admission; do not reserve zero. Estimate error should not silently become extra quota. Requests whose estimated demand exceeds a limit even in an otherwise empty window cannot become eligible by waiting; return a Batch recommendation to callers that allow that fallback, or a clear error.

In the current process-local implementation, completed reported token usage reconciles the in-memory rolling token ledger. Durable reconciliation after restart is future work. The longer-term ledger should also:

- Settle the reservation using reported input, cached input, output, and total usage when available.
- Otherwise retain the conservative estimate and mark reconciliation as estimated.
- Incorporate provider rate-limit remaining/reset headers and `Retry-After` as observations, not as proof that the local quota ledger is exact.
- Release reservations for requests known to have failed before dispatch. For an ambiguous timeout after bytes may have reached the provider, retain an uncertain attempt record and avoid an automatic duplicate unless the provider supports an idempotency contract.
- Rebuild window counters from durable attempt records after restart; bound ledger retention only after the associated quota windows have expired.

Token estimates and headers are imperfect. Provider-side traffic outside Kaiion, undocumented shared limits, cache behavior, and model aliasing can all make local capacity differ from provider capacity. The scheduler should expose estimate-versus-observed usage and upstream 429s so operators can tune conservative headroom.

## Cache and interaction signals

Cache affinity is a scheduling preference, not a correctness condition. Scope it by provider, credential, and stable Kaiion session. The first heuristic uses the prior completed response's observed cache-read usage and a configured cache-affinity window; it does not claim to know the provider's cache key or control provider cache state. A recent prompt window raises priority for a new user turn, while tool output is downweighted. Use later provider usage feedback to calibrate whether the heuristic was useful.

Count a follow-up as interactive only when the request carries a new user prompt. Tool outputs and other machine-generated continuations are downweighted and do not receive the same boost. Store a short-lived activity timestamp/priority state, not raw user text. A fast user follow-up earns a temporary boost; inactivity decays it, while queue aging prevents starvation. Session signals are scoped to the authenticated client/provider identity so unrelated users cannot inherit priority.

## Pluggable complexity classifier

Keep classifier access behind a small interface that accepts normalized request metadata and returns one of three bounded classes: low, medium, or high. The built-in heuristic uses estimated input tokens, tool count, and high/xhigh/max reasoning effort; a custom implementation can be installed in-process through the synchronous classifier trait. Custom classifiers must be fast and nonblocking because they run during queue ordering. Remote classifier services are not part of the first slice.

The classifier must not rewrite inference content or select an unrequested model. Complexity is a policy signal only. Track classifier latency, fallback count, and priority changes to evaluate whether it improves completion latency without starving simple or interactive work.

## `async-` virtual models

A model name prefixed with `async-` is an opt-in wait-tolerant alias. Kaiion removes exactly one prefix when constructing the upstream request and uses the remainder as the real provider model. When scheduling is enabled, the alias adds a lower-priority/longer-wait hint and may make batch fallback eligible under auto policy; with scheduling disabled, it is still resolved to the provider model but does not delay the live request. Do not send the virtual alias to the provider or advertise that it is a provider model. Reject an empty or otherwise invalid resolved model before queueing.

For example, a client may request `async-gpt-5.6`; Kaiion dispatches model `gpt-5.6` when the live scheduler admits it, using the longer wait configured for the alias. The upstream response uses the resolved provider model. Preserve alias metadata in diagnostics to make routing decisions auditable.

## Retries and fallback behavior

Queue delay is not a provider failure. In the first slice, Kaiion waits before sending HTTP headers; it does not emit a synthetic SSE progress heartbeat, so a client timeout can close the connection during a long wait. The proxy owns upstream retries; `max_retries` is the number of additional attempts (2 by default). Each retry re-enters quota scheduling. Kaiion honors an upstream `Retry-After` only when the delay fits the finite wait cap; once that cap or retry count is exceeded, it returns a bounded fallback/error result. An ambiguous connection loss after possible acceptance must not issue a duplicate live request unless the upstream idempotency contract makes that safe. Durable attempt tracking and provider idempotency remain future work.

When a low-priority request is eligible for Batch, move it before live dispatch and return the established batch-compatible response behavior. Do not start live work and then silently duplicate it in Batch. Requests that are not batch-compatible, lack a safe identity, exceed the queue bound, or miss their deadline should receive an explicit actionable error with a Kaiion request/job identifier and retry guidance. Never downgrade the model or silently ignore a quota limit to avoid a client-visible error.

## Operations and limits

The current deployment should use one Kaiion process per configured provider and leave headroom for calls outside Kaiion. It has no durable scheduled queue or provider usage ledger. In a future persistent scheduler, store queued request metadata, reservations, attempt state, and the resolved model so a restart can recover without creating a second provider call. Credentials must remain transient request headers; persist only a credential fingerprint for isolation.

Expose at least these operational signals as the scheduler grows: queue depth and oldest wait, queue time by priority/lane, current window reservations and observed usage per provider/model, estimated-versus-reported tokens, retries and uncertain attempts, upstream 429/5xx rates, cache-affinity hits/misses, alias use, classifier fallback, and admission/fallback reasons. Health checks should distinguish a live process from a scheduler that is accepting work.

Bound queue count/bytes, per-session occupancy, classifier time, maximum wait, retry count, and concurrent live dispatches. Define a full-queue policy and a graceful-shutdown behavior. Read scheduling policy on startup and require an explicit reload/restart contract. Do not promise multi-process correctness until cross-process quota reservations and scheduler leases are implemented.

## Staged implementation and release gates

### Stage 1: In-memory scheduled-live policy (current slice)

The current slice accepts an optional scheduling policy, applies matching provider/model request and token limits, spaces configured `max_rps` evenly, and bounds direct-mode wait. It includes session priority based on prior cache-read observations and recent user prompts, idle decay and queue aging, the built-in complexity heuristic behind a `ComplexityClassifier` trait, and wait-tolerant `async-` aliases. In auto mode, the scheduler can recommend Batch for a compatible low-priority cache miss or async request. Explicit direct mode waits within its configured bound; an overlarge request returns a Batch recommendation to callers that can choose Batch, or an error.

This slice is process-local, bounds the waiting queue by entry count and serialized request bytes, and waits before sending HTTP headers. It does not durably queue requests, restore quota reservations after restart, reconcile provider usage into a durable ledger, coordinate multiple Kaiion processes, or send a synthetic SSE heartbeat while waiting. Set `max_retries` conservatively: retries after ambiguous upstream acceptance still require provider idempotency to avoid duplicate work.

**Gate:** focused tests cover composed provider/model limits, sliding request counts, paced `max_rps`, token caps, bounded wait, priority scoring/aging, batch recommendation, alias resolution, and retry ceilings. The README and policy example must state the process-local and client-timeout limits.

### Stage 2: Durable queue and quota reconciliation

Persist queued request metadata, quota reservations, and attempt state without storing credentials. Replays should attach to the existing scheduled entry. Settle estimates from observed token usage, cache reads, rate-limit headers, and `Retry-After`; preserve uncertain acceptance distinctly from a retryable failure. Recover scheduled work after restart without creating duplicate live calls.

**Gate:** process restart tests prove waiting work is not lost and replays cannot create duplicate provider calls; ambiguous acceptance remains explicit; settled quota windows match recorded usage within documented estimation error.

### Stage 3: Client wait and operator controls

Design a client-facing wait contract for long delays. Preserve the existing upstream HTTP status/body and SSE framing; an in-progress heartbeat or detached scheduled-job response needs explicit compatibility tests because the current path waits before returning headers. Add per-session occupancy controls, queue/usage diagnostics, stronger cancellation/expiry behavior, and a documented policy reload contract.

**Gate:** real client timeout tests cover both JSON and SSE, full queues have actionable behavior, operators can distinguish active service from a stalled scheduler, and queue limits remain bounded under load.

### Stage 4: Shared limits and classifier extensions

Evaluate cross-process coordination using shared atomic reservations and scheduler leases only after the one-process durable design is proven. If external classifier integrations are needed, add a separately controlled adapter with strict latency, privacy, failure, and fallback limits; the current extension point is an in-process Rust trait.

**Gate:** soak tests include bursty and sustained traffic, provider throttling, process restarts, ambiguous transport failures, multiple Kaiion workers, and mixed interactive/async sessions; published limits match measured behavior.

The first release should describe limits plainly: cache hits are provider-controlled, token estimates are approximate, quotas seen by Kaiion may be shared with other clients, and ambiguous upstream acceptance cannot be retried safely without provider idempotency.
