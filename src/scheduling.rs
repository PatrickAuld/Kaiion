use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{Mutex, oneshot},
    time,
};

use crate::scheduling_policy::{ComplexitySettings, QuotaLimit, SchedulingPolicy};

const ASYNC_MODEL_PREFIX: &str = "async-";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionKind {
    UserPrompt,
    ToolOutput,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Complexity {
    Low,
    Medium,
    High,
}

/// Implement this trait to install an in-process workload classifier. The
/// built-in classifier uses token estimates, reasoning effort, and tool count.
pub trait ComplexityClassifier: Send + Sync + 'static {
    fn classify(&self, request: &ScheduleRequest) -> Complexity;
}

#[derive(Clone, Debug)]
pub struct HeuristicComplexityClassifier {
    settings: ComplexitySettings,
}

impl HeuristicComplexityClassifier {
    pub fn new(settings: ComplexitySettings) -> Self {
        Self { settings }
    }
}

impl ComplexityClassifier for HeuristicComplexityClassifier {
    fn classify(&self, request: &ScheduleRequest) -> Complexity {
        let input_tokens = request.input_tokens;
        let tools = request
            .body
            .as_ref()
            .and_then(|body| body.get("tools"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let high_reasoning = request
            .body
            .as_ref()
            .and_then(|body| body.pointer("/reasoning/effort"))
            .and_then(Value::as_str)
            .is_some_and(|effort| matches!(effort, "high" | "xhigh" | "max"));
        if high_reasoning
            || input_tokens >= self.settings.high_input_tokens
            || tools >= self.settings.high_tool_count
        {
            Complexity::High
        } else if input_tokens <= self.settings.low_input_tokens && tools <= 1 {
            Complexity::Low
        } else {
            Complexity::Medium
        }
    }
}

/// Values required by the scheduler. `auth_scope` must be a stable fingerprint,
/// never a raw API key. The session id must stay stable across turns.
#[derive(Clone, Debug)]
pub struct ScheduleRequest {
    pub provider: String,
    pub auth_scope: String,
    pub model: String,
    pub session_id: Option<String>,
    pub input_tokens: u64,
    /// A zero estimate is replaced with `SchedulingPolicy::default_output_tokens`.
    pub output_tokens: u64,
    pub interaction: InteractionKind,
    pub async_alias: bool,
    /// Most recent cache-read observation, if known. Completion observations
    /// are stored and used for later turns in the session.
    pub cache_read_hit: Option<bool>,
    /// Zero is the initial request; retries are numbered starting at one.
    pub retry_attempt: u32,
    pub upstream_retry_after: Option<Duration>,
    /// Set false for explicit Direct mode; the scheduler then waits for live
    /// quota instead of recommending Batch.
    pub allow_batch_fallback: bool,
    /// Optional original request JSON, supplied to the classifier.
    pub body: Option<Value>,
}

#[derive(Debug)]
pub enum ScheduleOutcome {
    Dispatch(SchedulePermit),
    RecommendBatch {
        reason: &'static str,
        retry_after: Option<Duration>,
    },
    RetryExhausted {
        reason: &'static str,
        retry_after: Option<Duration>,
    },
}

/// A quota reservation returned when a request may be sent live. Consume it
/// once with `Scheduler::complete` to reconcile token usage and cache outcome.
#[derive(Debug)]
pub struct SchedulePermit {
    reservation_id: Option<u64>,
    lease: Option<PermitLease>,
    provider: String,
    auth_scope: String,
    model: String,
    session_id: Option<String>,
    interaction: InteractionKind,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub waited: Duration,
}

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("invalid scheduling policy: {0}")]
    InvalidPolicy(String),
    #[error("provider, model, and auth scope must be non-empty")]
    InvalidRequest,
    #[error("scheduler waiter was cancelled before dispatch")]
    WaiterCancelled,
}

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Inner>,
}

struct Inner {
    policy: SchedulingPolicy,
    classifier: Arc<dyn ComplexityClassifier>,
    state: Mutex<State>,
}

struct PendingGuard {
    inner: Arc<Inner>,
    id: u64,
    armed: bool,
}

struct PermitLease {
    inner: Arc<Inner>,
    id: u64,
    armed: bool,
}

impl fmt::Debug for PermitLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermitLease")
            .field("id", &self.id)
            .field("armed", &self.armed)
            .finish()
    }
}

impl Drop for PermitLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = self.inner.clone();
        let id = self.id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut state = inner.state.lock().await;
                if state.active.remove(&id).is_some() {
                    dispatch_available(&inner, &mut state, Instant::now());
                }
            });
        }
    }
}

impl PendingGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = self.inner.clone();
        let id = self.id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut state = inner.state.lock().await;
                state.remove_pending(&id);
                if state.pending.is_empty() {
                    state.coalesce_until = None;
                }
                let now = Instant::now();
                prune_events(&mut state, &inner.policy, now);
                dispatch_available(&inner, &mut state, now);
            });
        }
    }
}

#[derive(Default)]
struct State {
    events: VecDeque<UsageEvent>,
    pending: HashMap<u64, Pending>,
    active: HashMap<u64, ActiveRequest>,
    pending_bytes: usize,
    pacing: HashMap<PacingKey, Instant>,
    sessions: HashMap<String, SessionState>,
    next_id: u64,
    coalesce_until: Option<Instant>,
}

#[derive(Clone)]
struct UsageEvent {
    id: u64,
    at: Instant,
    provider: String,
    auth_scope: String,
    model: String,
    input_tokens: u64,
    output_tokens: u64,
}

#[derive(Clone)]
struct ActiveRequest {
    provider: String,
    auth_scope: String,
    model: String,
}

struct Pending {
    request: ScheduleRequest,
    enqueued_at: Instant,
    input_tokens: u64,
    output_tokens: u64,
    bytes: usize,
    sender: oneshot::Sender<ScheduleOutcome>,
}

impl State {
    fn remove_pending(&mut self, id: &u64) -> Option<Pending> {
        let pending = self.pending.remove(id)?;
        self.pending_bytes = self.pending_bytes.saturating_sub(pending.bytes);
        Some(pending)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct PacingKey {
    rule_index: usize,
    provider: String,
    auth_scope: String,
    model: String,
    metric: PaceMetric,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
enum PaceMetric {
    Requests,
    InputTokens,
    OutputTokens,
    TotalTokens,
}

#[derive(Clone, Debug)]
struct Fit {
    at: Option<Instant>,
    impossible: bool,
}

impl Scheduler {
    pub fn new(policy: SchedulingPolicy) -> Result<Self, ScheduleError> {
        let classifier = Arc::new(HeuristicComplexityClassifier::new(
            policy.complexity.clone(),
        ));
        Self::with_classifier(policy, classifier)
    }

    pub fn with_classifier(
        policy: SchedulingPolicy,
        classifier: Arc<dyn ComplexityClassifier>,
    ) -> Result<Self, ScheduleError> {
        policy.validate().map_err(ScheduleError::InvalidPolicy)?;
        Ok(Self {
            inner: Arc::new(Inner {
                policy,
                classifier,
                state: Mutex::new(State::default()),
            }),
        })
    }

    pub fn policy(&self) -> &SchedulingPolicy {
        &self.inner.policy
    }

    /// Return an immediate fallback recommendation without claiming quota.
    /// This is useful for non-blocking route explanations; `acquire` remains
    /// the authoritative path because it reserves capacity atomically.
    pub async fn preview(&self, request: &ScheduleRequest) -> Option<&'static str> {
        if !self.inner.policy.enabled || !request.allow_batch_fallback {
            return None;
        }
        let state = self.inner.state.lock().await;
        self.batch_reason(request, &state, Instant::now())
    }

    /// Wait for a fair live slot up to the configured finite cap. Retry-After
    /// is honored when passed on a retry; this method does not itself resend an
    /// upstream request.
    pub async fn acquire(
        &self,
        mut request: ScheduleRequest,
    ) -> Result<ScheduleOutcome, ScheduleError> {
        if request.provider.trim().is_empty()
            || request.auth_scope.trim().is_empty()
            || request.model.trim().is_empty()
        {
            return Err(ScheduleError::InvalidRequest);
        }
        if request.output_tokens == 0 {
            request.output_tokens = self.inner.policy.default_output_tokens;
        }
        let started = Instant::now();
        let max_wait = self.inner.policy.max_wait(request.async_alias);
        let deadline = started + max_wait;

        if !self.inner.policy.enabled {
            return Ok(ScheduleOutcome::Dispatch(SchedulePermit {
                reservation_id: None,
                lease: None,
                provider: request.provider,
                auth_scope: request.auth_scope,
                model: request.model,
                session_id: request.session_id,
                interaction: request.interaction,
                input_tokens: request.input_tokens,
                output_tokens: request.output_tokens,
                waited: Duration::ZERO,
            }));
        }

        if let Some(reason) = self.preview(&request).await {
            return Ok(ScheduleOutcome::RecommendBatch {
                reason,
                retry_after: None,
            });
        }

        if request.retry_attempt > self.inner.policy.max_retries {
            return Ok(ScheduleOutcome::RetryExhausted {
                reason: "max_retries_exceeded",
                retry_after: request.upstream_retry_after,
            });
        }
        if let Some(retry_after) = request.upstream_retry_after {
            if retry_after > max_wait {
                return Ok(ScheduleOutcome::RetryExhausted {
                    reason: "retry_after_exceeds_wait_cap",
                    retry_after: Some(retry_after),
                });
            }
            if !retry_after.is_zero() {
                time::sleep(retry_after).await;
            }
        }

        let output_tokens = request.output_tokens;
        let request_bytes = request
            .body
            .as_ref()
            .map_or(0, |body| body.to_string().len());
        let (sender, mut receiver) = oneshot::channel();
        let id;
        {
            let mut state = self.inner.state.lock().await;
            let now = Instant::now();
            prune_events(&mut state, &self.inner.policy, now);
            if state.pending.len() >= self.inner.policy.max_pending
                || request_bytes
                    > self
                        .inner
                        .policy
                        .max_pending_bytes
                        .saturating_sub(state.pending_bytes)
            {
                return Ok(self.capacity_outcome(
                    &request,
                    "queue_full",
                    None,
                    now,
                    now,
                    Some(&state),
                ));
            }
            id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            if state.pending.is_empty() {
                state.coalesce_until =
                    Some(now + Duration::from_millis(self.inner.policy.queue_coalesce_ms));
            }
            state.pending_bytes += request_bytes;
            state.pending.insert(
                id,
                Pending {
                    request: request.clone(),
                    enqueued_at: now,
                    input_tokens: request.input_tokens,
                    output_tokens,
                    bytes: request_bytes,
                    sender,
                },
            );
        }
        let mut pending_guard = PendingGuard {
            inner: self.inner.clone(),
            id,
            armed: true,
        };

        loop {
            let now = Instant::now();
            let mut state = self.inner.state.lock().await;
            prune_events(&mut state, &self.inner.policy, now);

            // An individually oversized request cannot ever fit in a window.
            let this_fit = readiness(
                &self.inner.policy,
                &state,
                &request,
                request.input_tokens,
                output_tokens,
                now,
            );
            if this_fit.impossible {
                let outcome = self.capacity_outcome(
                    &request,
                    "request_exceeds_quota",
                    this_fit.at.map(|at| at.saturating_duration_since(now)),
                    now,
                    started,
                    Some(&state),
                );
                state.remove_pending(&id);
                pending_guard.disarm();
                if state.pending.is_empty() {
                    state.coalesce_until = None;
                }
                drop(state);
                return Ok(outcome);
            }

            let due = dispatch_ready(&self.inner, &mut state, now);
            let remaining = deadline.saturating_duration_since(now);
            let sleep_for = due
                .map(|at| at.saturating_duration_since(now))
                .unwrap_or(remaining)
                .min(remaining);
            drop(state);

            if let Ok(outcome) = receiver.try_recv() {
                pending_guard.disarm();
                return Ok(outcome);
            }
            if Instant::now() >= deadline {
                let mut state = self.inner.state.lock().await;
                let now = Instant::now();
                let retry_after = readiness(
                    &self.inner.policy,
                    &state,
                    &request,
                    request.input_tokens,
                    output_tokens,
                    now,
                )
                .at
                .map(|at| at.saturating_duration_since(now));
                let outcome = self.capacity_outcome(
                    &request,
                    "max_wait_exceeded",
                    retry_after,
                    now,
                    started,
                    Some(&state),
                );
                state.remove_pending(&id);
                pending_guard.disarm();
                if state.pending.is_empty() {
                    state.coalesce_until = None;
                }
                drop(state);
                return Ok(outcome);
            }
            if sleep_for.is_zero() {
                tokio::task::yield_now().await;
                continue;
            }

            tokio::select! {
                outcome = &mut receiver => {
                    pending_guard.disarm();
                    return outcome.map_err(|_| ScheduleError::WaiterCancelled);
                },
                _ = time::sleep(sleep_for) => {},
            }
        }
    }

    /// Reconcile a conservative reservation against actual usage and retain
    /// cache-read observations for the next turn in the same session.
    pub async fn complete(
        &self,
        mut permit: SchedulePermit,
        actual_input_tokens: Option<u64>,
        actual_output_tokens: Option<u64>,
        cache_read_hit: Option<bool>,
    ) {
        if let Some(lease) = permit.lease.as_mut() {
            lease.armed = false;
        }
        let now = Instant::now();
        let mut state = self.inner.state.lock().await;
        if let Some(id) = permit.reservation_id {
            state.active.remove(&id);
        }
        if let Some(id) = permit.reservation_id
            && let Some(event) = state.events.iter_mut().find(|event| event.id == id)
        {
            if let Some(input) = actual_input_tokens {
                event.input_tokens = input;
            }
            if let Some(output) = actual_output_tokens {
                event.output_tokens = output;
            }
        }
        if let Some(session_id) = permit.session_id {
            let key = session_key(&permit.provider, &permit.auth_scope, &session_id);
            let previous_cache = state.sessions.get(&key).and_then(|session| {
                if session.last_model == permit.model {
                    session.cache_read_hit
                } else {
                    None
                }
            });
            state.sessions.insert(
                key,
                SessionState {
                    last_activity: now,
                    last_interaction: permit.interaction,
                    last_model: permit.model,
                    cache_read_hit: cache_read_hit.or(previous_cache),
                },
            );
        }
        prune_events(&mut state, &self.inner.policy, now);
        dispatch_available(&self.inner, &mut state, now);
    }

    /// Release a reservation when the request is known not to have reached the
    /// upstream. Use this only for failures before any request bytes were sent.
    pub async fn abort(&self, mut permit: SchedulePermit) {
        if let Some(lease) = permit.lease.as_mut() {
            lease.armed = false;
        }
        let Some(id) = permit.reservation_id else {
            return;
        };
        let mut state = self.inner.state.lock().await;
        state.active.remove(&id);
        state.events.retain(|event| event.id != id);
        rebuild_pacing(&self.inner.policy, &mut state);
        let now = Instant::now();
        prune_events(&mut state, &self.inner.policy, now);
        dispatch_available(&self.inner, &mut state, now);
    }

    fn capacity_outcome(
        &self,
        request: &ScheduleRequest,
        reason: &'static str,
        retry_after: Option<Duration>,
        now: Instant,
        enqueued_at: Instant,
        state: Option<&State>,
    ) -> ScheduleOutcome {
        if let Some(reason) = state
            .and_then(|state| self.batch_reason_with_read(request, Some(state), enqueued_at, now))
        {
            ScheduleOutcome::RecommendBatch {
                reason,
                retry_after,
            }
        } else {
            ScheduleOutcome::RetryExhausted {
                reason,
                retry_after,
            }
        }
    }

    fn batch_reason(
        &self,
        request: &ScheduleRequest,
        state: &State,
        now: Instant,
    ) -> Option<&'static str> {
        self.batch_reason_with_read(request, Some(state), now, now)
    }

    fn batch_reason_with_read(
        &self,
        request: &ScheduleRequest,
        state: Option<&State>,
        enqueued_at: Instant,
        now: Instant,
    ) -> Option<&'static str> {
        batch_recommendation(
            &self.inner.policy,
            self.inner.classifier.as_ref(),
            request,
            state,
            enqueued_at,
            now,
        )
    }
}

#[derive(Clone, Debug)]
struct SessionState {
    last_activity: Instant,
    last_interaction: InteractionKind,
    last_model: String,
    cache_read_hit: Option<bool>,
}

fn batch_recommendation(
    policy: &SchedulingPolicy,
    classifier: &dyn ComplexityClassifier,
    request: &ScheduleRequest,
    state: Option<&State>,
    enqueued_at: Instant,
    now: Instant,
) -> Option<&'static str> {
    if !request.allow_batch_fallback || !policy.batch_on_cache_miss {
        return None;
    }
    let session = state.and_then(|state| {
        request.session_id.as_ref().and_then(|session| {
            state.sessions.get(&session_key(
                &request.provider,
                &request.auth_scope,
                session,
            ))
        })
    });
    let score = priority_score(policy, classifier, request, session, enqueued_at, now);
    let session_age = session.map(|session| now.saturating_duration_since(session.last_activity));
    if score >= i64::from(policy.high_priority_threshold) {
        return None;
    }
    if request.cache_read_hit == Some(false)
        || session.is_some_and(|session| {
            session.cache_read_hit == Some(false)
                && session.last_model == request.model
                && session_age.is_some_and(|age| {
                    age <= Duration::from_millis(policy.cache_affinity_window_ms)
                })
        })
    {
        Some("cache_miss_low_priority")
    } else if request.async_alias || request.model.starts_with(ASYNC_MODEL_PREFIX) {
        Some("async_alias_low_priority")
    } else {
        None
    }
}

fn dispatch_ready(inner: &Arc<Inner>, state: &mut State, now: Instant) -> Option<Instant> {
    let policy = &inner.policy;
    let queue_ready = state.coalesce_until.is_none_or(|at| now >= at);
    let mut best: Option<(u64, i64, Instant)> = None;
    let mut next_due: Option<Instant> = state.coalesce_until.filter(|at| *at > now);
    let mut impossible = Vec::new();

    for (id, pending) in &state.pending {
        if pending.sender.is_closed() {
            impossible.push(*id);
            continue;
        }
        let fit = readiness(
            policy,
            state,
            &pending.request,
            pending.input_tokens,
            pending.output_tokens,
            now,
        );
        if fit.impossible {
            impossible.push(*id);
            continue;
        }
        let Some(at) = fit.at else { continue };
        if at > now {
            next_due = Some(next_due.map_or(at, |current| current.min(at)));
            continue;
        }
        if !queue_ready {
            continue;
        }
        let session = pending.request.session_id.as_ref().and_then(|session| {
            state.sessions.get(&session_key(
                &pending.request.provider,
                &pending.request.auth_scope,
                session,
            ))
        });
        let score = priority_score(
            policy,
            inner.classifier.as_ref(),
            &pending.request,
            session,
            pending.enqueued_at,
            now,
        );
        if best
            .as_ref()
            .is_none_or(|(current_id, current_score, current_time)| {
                score > *current_score
                    || (score == *current_score && pending.enqueued_at < *current_time)
                    || (score == *current_score
                        && pending.enqueued_at == *current_time
                        && id < current_id)
            })
        {
            best = Some((*id, score, pending.enqueued_at));
        }
    }

    for id in impossible {
        if let Some(pending) = state.remove_pending(&id) {
            if pending.sender.is_closed() {
                continue;
            }
            let batch = batch_recommendation(
                &inner.policy,
                inner.classifier.as_ref(),
                &pending.request,
                Some(state),
                pending.enqueued_at,
                now,
            );
            let outcome = if let Some(reason) = batch {
                ScheduleOutcome::RecommendBatch {
                    reason,
                    retry_after: None,
                }
            } else {
                ScheduleOutcome::RetryExhausted {
                    reason: "request_exceeds_quota",
                    retry_after: None,
                }
            };
            let _ = pending.sender.send(outcome);
        }
    }

    if let Some((id, _, _)) = best {
        if let Some(pending) = state.remove_pending(&id) {
            if pending.sender.is_closed() {
                return if state.pending.is_empty() {
                    state.coalesce_until = None;
                    None
                } else {
                    Some(now)
                };
            }
            let reservation_id = id;
            state.events.push_back(UsageEvent {
                id: reservation_id,
                at: now,
                provider: pending.request.provider.clone(),
                auth_scope: pending.request.auth_scope.clone(),
                model: pending.request.model.clone(),
                input_tokens: pending.input_tokens,
                output_tokens: pending.output_tokens,
            });
            state.active.insert(
                reservation_id,
                ActiveRequest {
                    provider: pending.request.provider.clone(),
                    auth_scope: pending.request.auth_scope.clone(),
                    model: pending.request.model.clone(),
                },
            );
            let pacing_request = pending.request.clone();
            let reserved_input = pending.input_tokens;
            let reserved_output = pending.output_tokens;
            let waited = now.saturating_duration_since(pending.enqueued_at);
            let permit = SchedulePermit {
                reservation_id: Some(reservation_id),
                lease: Some(PermitLease {
                    inner: inner.clone(),
                    id: reservation_id,
                    armed: true,
                }),
                provider: pending.request.provider,
                auth_scope: pending.request.auth_scope,
                model: pending.request.model,
                session_id: pending.request.session_id,
                interaction: pending.request.interaction,
                input_tokens: pending.input_tokens,
                output_tokens: pending.output_tokens,
                waited,
            };
            if pending
                .sender
                .send(ScheduleOutcome::Dispatch(permit))
                .is_err()
            {
                state.events.retain(|event| event.id != reservation_id);
                state.active.remove(&reservation_id);
                return Some(now);
            }
            update_pacing(
                policy,
                state,
                &pacing_request,
                reserved_input,
                reserved_output,
                now,
            );
        }
        // Dispatch one at a time so the next loop observes the reservation and
        // does not issue a burst around a paced token or RPS budget.
        return Some(now);
    }

    if state.pending.is_empty() {
        state.coalesce_until = None;
        None
    } else {
        if queue_ready {
            state.coalesce_until = None;
        }
        next_due
    }
}

fn readiness(
    policy: &SchedulingPolicy,
    state: &State,
    request: &ScheduleRequest,
    input_tokens: u64,
    output_tokens: u64,
    now: Instant,
) -> Fit {
    if !policy.enabled {
        return Fit {
            at: Some(now),
            impossible: false,
        };
    }
    let mut ready = now;
    let total_tokens = input_tokens.saturating_add(output_tokens);
    for (rule_index, limit) in policy.limits.iter().enumerate() {
        if !rule_matches(limit, request) {
            continue;
        }
        if limit.max_input_tokens.is_some_and(|cap| input_tokens > cap)
            || limit
                .max_output_tokens
                .is_some_and(|cap| output_tokens > cap)
            || limit.max_total_tokens.is_some_and(|cap| total_tokens > cap)
        {
            return Fit {
                at: None,
                impossible: true,
            };
        }
        let events: Vec<&UsageEvent> = state
            .events
            .iter()
            .filter(|event| event_matches(limit, request, event, now))
            .collect();
        let rolling = rolling_fit(limit, &events, input_tokens, output_tokens, now);
        if rolling.impossible {
            return rolling;
        }
        if let Some(at) = rolling.at {
            ready = ready.max(at);
        }
        let key_base = PacingKey {
            rule_index,
            provider: request.provider.clone(),
            auth_scope: if limit.auth_scoped {
                request.auth_scope.clone()
            } else {
                String::new()
            },
            model: limit.model.clone().unwrap_or_default(),
            metric: PaceMetric::Requests,
        };
        if limit.max_rps.is_some() {
            let key = PacingKey {
                metric: PaceMetric::Requests,
                ..key_base.clone()
            };
            if let Some(at) = state.pacing.get(&key) {
                ready = ready.max(*at);
            }
        }
        for (metric, cap, tokens) in [
            (
                PaceMetric::InputTokens,
                limit.max_input_tokens,
                input_tokens,
            ),
            (
                PaceMetric::OutputTokens,
                limit.max_output_tokens,
                output_tokens,
            ),
            (
                PaceMetric::TotalTokens,
                limit.max_total_tokens,
                total_tokens,
            ),
        ] {
            if cap.is_some() && tokens > 0 {
                let key = PacingKey {
                    metric,
                    ..key_base.clone()
                };
                if let Some(at) = state.pacing.get(&key) {
                    ready = ready.max(*at);
                }
            }
        }
    }
    for limit in policy
        .limits
        .iter()
        .filter(|limit| limit.max_concurrency.is_some() && rule_matches(limit, request))
    {
        let active = state
            .active
            .values()
            .filter(|active| active_matches(limit, request, active))
            .count() as u64;
        if active >= limit.max_concurrency.unwrap_or_default() {
            return Fit {
                at: (ready > now).then_some(ready),
                impossible: false,
            };
        }
    }
    Fit {
        at: Some(ready),
        impossible: false,
    }
}

fn active_matches(limit: &QuotaLimit, request: &ScheduleRequest, active: &ActiveRequest) -> bool {
    active.provider == request.provider
        && (!limit.auth_scoped || active.auth_scope == request.auth_scope)
        && limit
            .model
            .as_ref()
            .is_none_or(|model| model == &active.model)
}

fn dispatch_available(inner: &Arc<Inner>, state: &mut State, now: Instant) {
    let mut due = dispatch_ready(inner, state, now);
    while due.is_some_and(|at| at <= now) {
        due = dispatch_ready(inner, state, now);
    }
}

fn rolling_fit(
    limit: &QuotaLimit,
    events: &[&UsageEvent],
    input_tokens: u64,
    output_tokens: u64,
    now: Instant,
) -> Fit {
    let mut request_cap = limit.max_requests;
    if let Some(rps) = limit.max_rps {
        let rps_cap = rps.saturating_mul(limit.window_seconds);
        request_cap = Some(request_cap.map_or(rps_cap, |cap| cap.min(rps_cap)));
    }
    let total_cap = limit.max_total_tokens;
    if request_cap.is_none()
        && limit.max_input_tokens.is_none()
        && limit.max_output_tokens.is_none()
        && total_cap.is_none()
    {
        return Fit {
            at: Some(now),
            impossible: false,
        };
    }
    let requested_total = input_tokens.saturating_add(output_tokens);
    let mut active: Vec<(Instant, u64, u64, u64)> = events
        .iter()
        .map(|event| {
            (
                event.at + Duration::from_secs(limit.window_seconds),
                1,
                event.input_tokens,
                event.output_tokens,
            )
        })
        .collect();
    active.sort_by_key(|event| event.0);
    let cap_fit = |reqs: u64, input: u64, output: u64| {
        request_cap.is_none_or(|cap| reqs.saturating_add(1) <= cap)
            && limit
                .max_input_tokens
                .is_none_or(|cap| input.saturating_add(input_tokens) <= cap)
            && limit
                .max_output_tokens
                .is_none_or(|cap| output.saturating_add(output_tokens) <= cap)
            && total_cap.is_none_or(|cap| {
                input.saturating_add(output).saturating_add(requested_total) <= cap
            })
    };
    let mut reqs = active.iter().map(|event| event.1).sum::<u64>();
    let mut input = active.iter().map(|event| event.2).sum::<u64>();
    let mut output = active.iter().map(|event| event.3).sum::<u64>();
    if cap_fit(reqs, input, output) {
        return Fit {
            at: Some(now),
            impossible: false,
        };
    }
    while let Some((expires_at, count, event_input, event_output)) = active.first().copied() {
        reqs = reqs.saturating_sub(count);
        input = input.saturating_sub(event_input);
        output = output.saturating_sub(event_output);
        active.remove(0);
        if cap_fit(reqs, input, output) {
            return Fit {
                at: Some(expires_at),
                impossible: false,
            };
        }
    }
    Fit {
        at: None,
        impossible: true,
    }
}

fn rule_matches(limit: &QuotaLimit, request: &ScheduleRequest) -> bool {
    limit
        .provider
        .as_ref()
        .is_none_or(|provider| provider == &request.provider)
        && limit
            .model
            .as_ref()
            .is_none_or(|model| model == &request.model)
}

fn event_matches(
    limit: &QuotaLimit,
    request: &ScheduleRequest,
    event: &UsageEvent,
    now: Instant,
) -> bool {
    now.saturating_duration_since(event.at) < Duration::from_secs(limit.window_seconds)
        && event.provider == request.provider
        && (!limit.auth_scoped || event.auth_scope == request.auth_scope)
        && limit
            .model
            .as_ref()
            .is_none_or(|model| model == &event.model)
}

fn update_pacing(
    policy: &SchedulingPolicy,
    state: &mut State,
    request: &ScheduleRequest,
    input_tokens: u64,
    output_tokens: u64,
    now: Instant,
) {
    let total_tokens = input_tokens.saturating_add(output_tokens);
    for (rule_index, limit) in policy.limits.iter().enumerate() {
        if !rule_matches(limit, request) {
            continue;
        }
        let base = PacingKey {
            rule_index,
            provider: request.provider.clone(),
            auth_scope: if limit.auth_scoped {
                request.auth_scope.clone()
            } else {
                String::new()
            },
            model: limit.model.clone().unwrap_or_default(),
            metric: PaceMetric::Requests,
        };
        if let Some(rps) = limit.max_rps {
            let key = PacingKey {
                metric: PaceMetric::Requests,
                ..base.clone()
            };
            let start = state.pacing.get(&key).copied().unwrap_or(now).max(now);
            state.pacing.insert(key, start + duration_for_rate(1, rps));
        }
        for (metric, cap, tokens) in [
            (
                PaceMetric::InputTokens,
                limit.max_input_tokens,
                input_tokens,
            ),
            (
                PaceMetric::OutputTokens,
                limit.max_output_tokens,
                output_tokens,
            ),
            (
                PaceMetric::TotalTokens,
                limit.max_total_tokens,
                total_tokens,
            ),
        ] {
            if let Some(cap) = cap.filter(|_| tokens > 0) {
                let key = PacingKey {
                    metric,
                    ..base.clone()
                };
                let start = state.pacing.get(&key).copied().unwrap_or(now).max(now);
                let seconds = (tokens as f64 * limit.window_seconds as f64) / cap as f64;
                let duration = duration_from_seconds(seconds);
                state.pacing.insert(key, start + duration);
            }
        }
    }
}

fn rebuild_pacing(policy: &SchedulingPolicy, state: &mut State) {
    state.pacing.clear();
    let events: Vec<UsageEvent> = state.events.iter().cloned().collect();
    for event in events {
        let request = ScheduleRequest {
            provider: event.provider,
            auth_scope: event.auth_scope,
            model: event.model,
            session_id: None,
            input_tokens: event.input_tokens,
            output_tokens: event.output_tokens,
            interaction: InteractionKind::Unknown,
            async_alias: false,
            cache_read_hit: None,
            retry_attempt: 0,
            upstream_retry_after: None,
            allow_batch_fallback: false,
            body: None,
        };
        update_pacing(
            policy,
            state,
            &request,
            event.input_tokens,
            event.output_tokens,
            event.at,
        );
    }
}

fn duration_for_rate(units: u64, rate_per_second: u64) -> Duration {
    duration_from_seconds((units as f64 / rate_per_second as f64).max(0.000_000_001))
}

fn duration_from_seconds(seconds: f64) -> Duration {
    const MAX_PACE_SECONDS: f64 = 31_536_000.0;
    if !seconds.is_finite() || seconds >= MAX_PACE_SECONDS {
        return Duration::from_secs(MAX_PACE_SECONDS as u64);
    }
    let whole = seconds.floor() as u64;
    let nanos = ((seconds - whole as f64) * 1_000_000_000.0) as u32;
    Duration::new(whole, nanos)
}

fn prune_events(state: &mut State, policy: &SchedulingPolicy, now: Instant) {
    let max_window = policy
        .limits
        .iter()
        .map(|limit| Duration::from_secs(limit.window_seconds))
        .max()
        .unwrap_or(Duration::ZERO);
    while state
        .events
        .front()
        .is_some_and(|event| now.saturating_duration_since(event.at) >= max_window)
    {
        state.events.pop_front();
    }
    state
        .pacing
        .retain(|_, at| *at > now || now.saturating_duration_since(*at) < max_window);
    state.sessions.retain(|_, session| {
        now.saturating_duration_since(session.last_activity)
            <= Duration::from_millis(policy.idle_decay_ms.saturating_mul(8))
    });
}

fn priority_score(
    policy: &SchedulingPolicy,
    classifier: &dyn ComplexityClassifier,
    request: &ScheduleRequest,
    session: Option<&SessionState>,
    enqueued_at: Instant,
    now: Instant,
) -> i64 {
    let mut score = match classifier.classify(request) {
        Complexity::Low => policy.complexity.low_priority,
        Complexity::Medium => policy.complexity.medium_priority,
        Complexity::High => policy.complexity.high_priority,
    } as i64;
    let session_age = session.map(|session| now.saturating_duration_since(session.last_activity));
    if request.cache_read_hit == Some(true)
        || (request.cache_read_hit.is_none()
            && session.is_some_and(|session| {
                session.cache_read_hit == Some(true)
                    && session.last_model == request.model
                    && session_age.is_some_and(|age| {
                        age <= Duration::from_millis(policy.cache_affinity_window_ms)
                    })
            }))
    {
        score += i64::from(policy.cache_affinity_boost);
    }
    if request.interaction == InteractionKind::UserPrompt
        && session_age
            .is_some_and(|age| age <= Duration::from_millis(policy.recent_prompt_window_ms))
    {
        score += i64::from(policy.recent_prompt_boost);
        if session.is_some_and(|session| session.last_interaction == InteractionKind::ToolOutput) {
            score += i64::from(policy.recent_prompt_boost / 2);
        }
    }
    if request.interaction == InteractionKind::ToolOutput {
        score -= i64::from(policy.tool_output_penalty);
    }
    if request.async_alias || request.model.starts_with(ASYNC_MODEL_PREFIX) {
        score -= i64::from(policy.async_alias_penalty);
    }
    if let Some(age) = session_age
        && age > Duration::from_millis(policy.idle_decay_ms)
        && policy.idle_decay_ms > 0
    {
        let idle_intervals = age.as_millis() / u128::from(policy.idle_decay_ms) - 1;
        let penalty = idle_intervals
            .saturating_mul(u128::from(
                policy.idle_decay_penalty_per_interval.max(0) as u32
            ))
            .min(1_000_000) as i64;
        score -= penalty;
    }
    let wait_intervals = now.saturating_duration_since(enqueued_at).as_millis()
        / u128::from(policy.aging_interval_ms.max(1));
    score += (wait_intervals
        .saturating_mul(u128::from(
            policy.queue_aging_boost_per_interval.max(0) as u32
        ))
        .min(1_000_000)) as i64;
    score
}

fn session_key(provider: &str, auth_scope: &str, session_id: &str) -> String {
    format!("{provider}\0{auth_scope}\0{session_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduling_policy::QuotaLimit;

    fn request(model: &str) -> ScheduleRequest {
        ScheduleRequest {
            provider: "openai".into(),
            auth_scope: "fingerprint-a".into(),
            model: model.into(),
            session_id: Some("session-1".into()),
            input_tokens: 100,
            output_tokens: 100,
            interaction: InteractionKind::UserPrompt,
            async_alias: false,
            cache_read_hit: None,
            retry_attempt: 0,
            upstream_retry_after: None,
            allow_batch_fallback: true,
            body: None,
        }
    }

    fn policy(limits: Vec<QuotaLimit>) -> SchedulingPolicy {
        SchedulingPolicy {
            enabled: true,
            limits,
            queue_coalesce_ms: 0,
            max_wait_ms: 2_000,
            async_max_wait_ms: 2_000,
            ..SchedulingPolicy::default()
        }
    }

    fn limit(model: Option<&str>, extra: impl FnOnce(&mut QuotaLimit)) -> QuotaLimit {
        let mut result = QuotaLimit {
            provider: None,
            model: model.map(str::to_string),
            auth_scoped: false,
            window_seconds: 1,
            max_rps: None,
            max_requests: None,
            max_input_tokens: None,
            max_output_tokens: None,
            max_total_tokens: None,
            max_concurrency: None,
        };
        extra(&mut result);
        result
    }

    #[tokio::test]
    async fn provider_wide_and_model_limits_compose_and_reconcile_usage() {
        let scheduler = Scheduler::new(policy(vec![
            limit(None, |limit| limit.max_requests = Some(2)),
            limit(Some("reasoner"), |limit| limit.max_input_tokens = Some(150)),
        ]))
        .unwrap();
        let first = match scheduler.acquire(request("reasoner")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler
            .complete(first, Some(50), Some(50), Some(true))
            .await;
        // The actual usage (50) was reconciled from the 100 token reservation.
        let second = match scheduler.acquire(request("reasoner")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(second, None, None, None).await;
    }

    #[tokio::test]
    async fn auth_aggregates_by_default_but_can_be_scoped_per_rule() {
        let mut aggregate_policy = policy(vec![limit(None, |limit| {
            limit.max_requests = Some(1);
        })]);
        aggregate_policy.max_wait_ms = 100;
        aggregate_policy.async_max_wait_ms = 100;
        let scheduler = Scheduler::new(aggregate_policy).unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, None).await;
        let mut other_auth = request("model");
        other_auth.auth_scope = "fingerprint-b".into();
        let outcome = scheduler.acquire(other_auth).await.unwrap();
        assert!(matches!(outcome, ScheduleOutcome::RetryExhausted { .. }));

        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.max_requests = Some(1);
            limit.auth_scoped = true;
        })]))
        .unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, None).await;
        let mut other_auth = request("model");
        other_auth.auth_scope = "fingerprint-b".into();
        assert!(matches!(
            scheduler.acquire(other_auth).await.unwrap(),
            ScheduleOutcome::Dispatch(_)
        ));
    }

    #[tokio::test]
    async fn max_rps_paces_requests_and_tokens_have_a_virtual_rate() {
        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.max_rps = Some(2);
            limit.max_input_tokens = Some(200);
            limit.window_seconds = 1;
        })]))
        .unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, None).await;
        let started = Instant::now();
        let second = scheduler.acquire(request("model")).await.unwrap();
        assert!(matches!(second, ScheduleOutcome::Dispatch(_)));
        assert!(started.elapsed() >= Duration::from_millis(450));
    }

    #[tokio::test]
    async fn long_token_window_preserves_multi_day_pacing() {
        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.window_seconds = 30 * 24 * 60 * 60;
            limit.max_input_tokens = Some(1_000);
        })]))
        .unwrap();
        let mut req = request("model");
        req.input_tokens = 100;
        req.output_tokens = 1;
        let permit = match scheduler.acquire(req).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        let state = scheduler.inner.state.lock().await;
        let next = state
            .pacing
            .iter()
            .find(|(key, _)| key.metric == PaceMetric::InputTokens)
            .map(|(_, next)| *next)
            .unwrap();
        assert!(
            next.saturating_duration_since(Instant::now()) > Duration::from_secs(2 * 24 * 60 * 60)
        );
        drop(state);
        scheduler.complete(permit, None, None, None).await;
    }

    #[tokio::test]
    async fn cancelled_waiter_is_removed_without_consuming_a_slot() {
        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.max_rps = Some(1);
        })]))
        .unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, None).await;

        let waiting_scheduler = scheduler.clone();
        let waiter = tokio::spawn(async move { waiting_scheduler.acquire(request("model")).await });
        time::sleep(Duration::from_millis(20)).await;
        waiter.abort();
        let _ = waiter.await;
        time::sleep(Duration::from_millis(20)).await;

        let state = scheduler.inner.state.lock().await;
        assert!(state.pending.is_empty());
        assert_eq!(state.events.len(), 1);
    }

    #[tokio::test]
    async fn provider_and_model_concurrency_limits_compose_without_head_of_line_blocking() {
        let scheduler = Scheduler::new(policy(vec![
            limit(None, |limit| limit.max_concurrency = Some(2)),
            limit(Some("local-model"), |limit| limit.max_concurrency = Some(1)),
        ]))
        .unwrap();
        let first = match scheduler.acquire(request("local-model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };

        let waiting_scheduler = scheduler.clone();
        let waiter =
            tokio::spawn(async move { waiting_scheduler.acquire(request("local-model")).await });
        time::timeout(Duration::from_secs(1), async {
            loop {
                if scheduler.inner.state.lock().await.pending.len() == 1 {
                    break;
                }
                time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("same-model request should wait for its concurrency slot");

        // A different model can use the second provider-wide slot while the
        // first model's own limit is full.
        let other_model = match scheduler.acquire(request("remote-model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, None).await;
        let queued = time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("completion should wake the queued request")
            .unwrap()
            .unwrap();
        let queued = match queued {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(other_model, None, None, None).await;
        scheduler.complete(queued, None, None, None).await;
    }

    #[tokio::test]
    async fn dropping_dispatched_permit_releases_its_concurrency_slot() {
        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.max_concurrency = Some(1);
        })]))
        .unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        let waiting_scheduler = scheduler.clone();
        let waiter = tokio::spawn(async move { waiting_scheduler.acquire(request("model")).await });
        time::timeout(Duration::from_secs(1), async {
            loop {
                if scheduler.inner.state.lock().await.pending.len() == 1 {
                    break;
                }
                time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("request should be waiting on the occupied slot");

        drop(first);
        let queued = time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("dropping a permit should release its slot")
            .unwrap()
            .unwrap();
        match queued {
            ScheduleOutcome::Dispatch(permit) => scheduler.complete(permit, None, None, None).await,
            outcome => panic!("unexpected {outcome:?}"),
        }
    }

    #[tokio::test]
    async fn abort_releases_concurrency_and_quota_reservations() {
        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.max_concurrency = Some(1);
            limit.max_requests = Some(1);
        })]))
        .unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.abort(first).await;
        let second = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(second, None, None, None).await;
    }

    #[tokio::test]
    async fn auth_scoped_concurrency_rules_keep_independent_slots() {
        let scheduler = Scheduler::new(policy(vec![limit(None, |limit| {
            limit.max_concurrency = Some(1);
            limit.auth_scoped = true;
        })]))
        .unwrap();
        let first = match scheduler.acquire(request("model")).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        let mut other_auth = request("model");
        other_auth.auth_scope = "fingerprint-b".into();
        let second = match scheduler.acquire(other_auth).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, None).await;
        scheduler.complete(second, None, None, None).await;
    }

    #[tokio::test]
    async fn oversized_queued_body_is_rejected_without_reserving_quota() {
        let mut policy = policy(vec![limit(None, |limit| limit.max_requests = Some(1))]);
        policy.max_pending_bytes = 16;
        let scheduler = Scheduler::new(policy).unwrap();
        let mut req = request("model");
        req.body = Some(serde_json::json!({"input": "a long queued request"}));
        assert!(matches!(
            scheduler.acquire(req).await.unwrap(),
            ScheduleOutcome::RetryExhausted {
                reason: "queue_full",
                ..
            }
        ));
        assert!(scheduler.inner.state.lock().await.events.is_empty());
    }

    #[tokio::test]
    async fn low_priority_cache_miss_can_recommend_batch_but_direct_waits() {
        let mut policy = policy(vec![]);
        policy.batch_on_cache_miss = true;
        policy.complexity.low_priority = -10;
        policy.high_priority_threshold = 20;
        let scheduler = Scheduler::new(policy).unwrap();
        let mut req = request("model");
        req.cache_read_hit = Some(false);
        assert!(matches!(
            scheduler.acquire(req.clone()).await.unwrap(),
            ScheduleOutcome::RecommendBatch {
                reason: "cache_miss_low_priority",
                ..
            }
        ));
        req.allow_batch_fallback = false;
        assert!(matches!(
            scheduler.acquire(req).await.unwrap(),
            ScheduleOutcome::Dispatch(_)
        ));
    }

    #[tokio::test]
    async fn session_priority_is_auth_scoped_and_async_alias_has_longer_cap() {
        let scheduler = Scheduler::new(policy(vec![])).unwrap();
        let mut first_req = request("model");
        first_req.allow_batch_fallback = false;
        let first = match scheduler.acquire(first_req).await.unwrap() {
            ScheduleOutcome::Dispatch(permit) => permit,
            outcome => panic!("unexpected {outcome:?}"),
        };
        scheduler.complete(first, None, None, Some(true)).await;
        let mut other = request("model");
        other.auth_scope = "different-auth".into();
        other.cache_read_hit = None;
        assert_eq!(scheduler.preview(&other).await, None);
        let mut alias = request("async-model");
        alias.interaction = InteractionKind::ToolOutput;
        assert_eq!(
            scheduler.batch_reason_with_read(&alias, None, Instant::now(), Instant::now()),
            Some("async_alias_low_priority")
        );
        assert!(scheduler.policy().async_max_wait_ms >= scheduler.policy().max_wait_ms);
    }
}
