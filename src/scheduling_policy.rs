use std::{path::Path, time::Duration};

use serde::{Deserialize, Serialize};

use crate::error::ProxyError;

const MAX_WINDOW_SECONDS: u64 = 31_536_000;
const MAX_WAIT_MS: u64 = 86_400_000;

/// Opt-in policy for live request pacing. Every matching limit is enforced;
/// provider and model limits therefore compose.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulingPolicy {
    pub enabled: bool,
    pub limits: Vec<QuotaLimit>,
    /// Maximum time a normal live request may wait before returning a bounded
    /// retry/fallback decision.
    pub max_wait_ms: u64,
    /// Longer wait cap for requests addressed to an `async-` mock model.
    pub async_max_wait_ms: u64,
    /// Number of upstream retries permitted after the initial attempt.
    pub max_retries: u32,
    /// Upper bound for the in-memory waiting queue.
    pub max_pending: usize,
    /// Approximate serialized request bytes held by waiting entries.
    pub max_pending_bytes: usize,
    /// Small initial collection interval so concurrently arriving requests can
    /// be ordered by priority before quota capacity is assigned.
    pub queue_coalesce_ms: u64,
    /// Priority boost when a session's previous response reported a cache read.
    pub cache_affinity_window_ms: u64,
    /// Window in which a user prompt following recent session activity receives
    /// a priority boost.
    pub recent_prompt_window_ms: u64,
    /// Delay after which session priority decays.
    pub idle_decay_ms: u64,
    /// How often a queued request gains one point of aging priority.
    pub aging_interval_ms: u64,
    /// Priority threshold above which a request is treated as high priority.
    pub high_priority_threshold: i32,
    pub cache_affinity_boost: i32,
    pub recent_prompt_boost: i32,
    pub tool_output_penalty: i32,
    pub idle_decay_penalty_per_interval: i32,
    pub queue_aging_boost_per_interval: i32,
    pub async_alias_penalty: i32,
    /// Recommend Batch for a known cache miss from a below-threshold request
    /// when the caller allows fallback.
    pub batch_on_cache_miss: bool,
    /// Conservative output reservation used when the request omitted a limit.
    pub default_output_tokens: u64,
    pub complexity: ComplexitySettings,
}

impl Default for SchedulingPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            limits: Vec::new(),
            max_wait_ms: 30_000,
            async_max_wait_ms: 180_000,
            max_retries: 2,
            max_pending: 10_000,
            max_pending_bytes: 134_217_728,
            queue_coalesce_ms: 2,
            cache_affinity_window_ms: 60_000,
            recent_prompt_window_ms: 10_000,
            idle_decay_ms: 120_000,
            aging_interval_ms: 1_000,
            high_priority_threshold: 30,
            cache_affinity_boost: 40,
            recent_prompt_boost: 30,
            tool_output_penalty: 15,
            idle_decay_penalty_per_interval: 5,
            queue_aging_boost_per_interval: 5,
            async_alias_penalty: 20,
            batch_on_cache_miss: true,
            default_output_tokens: 16_384,
            complexity: ComplexitySettings::default(),
        }
    }
}

/// A rolling quota for one provider, one model, or both. An omitted provider
/// or model matches each value independently. `max_rps` and token caps are
/// paced evenly; `max_requests` is a strict rolling count and may allow a
/// burst up to that count.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaLimit {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    /// Provider/model totals aggregate across credentials by default. Set this
    /// to true to maintain this rule independently for each auth fingerprint.
    #[serde(default)]
    pub auth_scoped: bool,
    pub window_seconds: u64,
    #[serde(default)]
    pub max_rps: Option<u64>,
    #[serde(default)]
    pub max_requests: Option<u64>,
    #[serde(default)]
    pub max_input_tokens: Option<u64>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub max_total_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComplexitySettings {
    pub low_input_tokens: u64,
    pub high_input_tokens: u64,
    pub high_tool_count: usize,
    pub low_priority: i32,
    pub medium_priority: i32,
    pub high_priority: i32,
}

impl Default for ComplexitySettings {
    fn default() -> Self {
        Self {
            low_input_tokens: 2_048,
            high_input_tokens: 16_384,
            high_tool_count: 4,
            low_priority: 10,
            medium_priority: 0,
            high_priority: -15,
        }
    }
}

impl SchedulingPolicy {
    pub fn load(path: Option<&Path>) -> Result<Self, ProxyError> {
        let policy = match path {
            Some(path) => serde_json::from_slice(&std::fs::read(path).map_err(|error| {
                ProxyError::BadRequest(format!(
                    "cannot read scheduling policy {}: {error}",
                    path.display()
                ))
            })?)?,
            None => Self::default(),
        };
        policy.validate().map_err(|error| {
            ProxyError::BadRequest(format!("invalid scheduling policy: {error}"))
        })?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.max_wait_ms == 0 || self.max_wait_ms > MAX_WAIT_MS {
            return Err(format!("max_wait_ms must be between 1 and {MAX_WAIT_MS}"));
        }
        if self.async_max_wait_ms == 0 || self.async_max_wait_ms > MAX_WAIT_MS {
            return Err(format!(
                "async_max_wait_ms must be between 1 and {MAX_WAIT_MS}"
            ));
        }
        if self.aging_interval_ms == 0 {
            return Err("aging_interval_ms must be greater than zero".into());
        }
        if self.queue_coalesce_ms > self.max_wait_ms {
            return Err("queue_coalesce_ms cannot exceed max_wait_ms".into());
        }
        if self.default_output_tokens == 0 {
            return Err("default_output_tokens must be greater than zero".into());
        }
        if self.max_pending == 0 || self.max_pending > 1_000_000 {
            return Err("max_pending must be between 1 and 1000000".into());
        }
        if self.max_pending_bytes == 0 || self.max_pending_bytes as u64 > 4_294_967_296 {
            return Err("max_pending_bytes must be between 1 and 4294967296".into());
        }
        for (name, value) in [
            ("high_priority_threshold", self.high_priority_threshold),
            ("cache_affinity_boost", self.cache_affinity_boost),
            ("recent_prompt_boost", self.recent_prompt_boost),
            ("tool_output_penalty", self.tool_output_penalty),
            (
                "idle_decay_penalty_per_interval",
                self.idle_decay_penalty_per_interval,
            ),
            (
                "queue_aging_boost_per_interval",
                self.queue_aging_boost_per_interval,
            ),
            ("async_alias_penalty", self.async_alias_penalty),
            ("complexity.low_priority", self.complexity.low_priority),
            (
                "complexity.medium_priority",
                self.complexity.medium_priority,
            ),
            ("complexity.high_priority", self.complexity.high_priority),
        ] {
            if value.unsigned_abs() > 1_000_000 {
                return Err(format!("{name} must be between -1000000 and 1000000"));
            }
        }
        if self.complexity.high_input_tokens <= self.complexity.low_input_tokens {
            return Err("complexity.high_input_tokens must exceed low_input_tokens".into());
        }
        if self.complexity.high_tool_count == 0 {
            return Err("complexity.high_tool_count must be greater than zero".into());
        }

        for (index, limit) in self.limits.iter().enumerate() {
            if limit.window_seconds == 0 || limit.window_seconds > MAX_WINDOW_SECONDS {
                return Err(format!(
                    "limits[{index}].window_seconds must be between 1 and {MAX_WINDOW_SECONDS}"
                ));
            }
            if limit.max_requests.is_none()
                && limit.max_rps.is_none()
                && limit.max_input_tokens.is_none()
                && limit.max_output_tokens.is_none()
                && limit.max_total_tokens.is_none()
            {
                return Err(format!("limits[{index}] must define at least one quota"));
            }
            if limit.max_rps == Some(0)
                || limit.max_requests == Some(0)
                || limit.max_input_tokens == Some(0)
                || limit.max_output_tokens == Some(0)
                || limit.max_total_tokens == Some(0)
            {
                return Err(format!(
                    "limits[{index}] quota values must be greater than zero"
                ));
            }
            if limit
                .provider
                .as_deref()
                .is_some_and(|provider| provider.trim().is_empty())
                || limit
                    .model
                    .as_deref()
                    .is_some_and(|model| model.trim().is_empty())
            {
                return Err(format!("limits[{index}] provider/model cannot be blank"));
            }
            for (field, value) in [
                ("max_rps", limit.max_rps),
                ("max_requests", limit.max_requests),
                ("max_input_tokens", limit.max_input_tokens),
                ("max_output_tokens", limit.max_output_tokens),
                ("max_total_tokens", limit.max_total_tokens),
            ] {
                if value.is_some_and(|amount| amount > i64::MAX as u64) {
                    return Err(format!("limits[{index}].{field} is too large"));
                }
            }
        }
        Ok(())
    }

    pub fn max_wait(&self, async_alias: bool) -> Duration {
        Duration::from_millis(if async_alias {
            self.async_max_wait_ms.max(self.max_wait_ms)
        } else {
            self.max_wait_ms
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_are_opt_in_bounded_and_conservative() {
        let policy = SchedulingPolicy::default();
        assert!(!policy.enabled);
        assert_eq!(policy.max_retries, 2);
        assert_eq!(policy.default_output_tokens, 16_384);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn parses_composable_provider_and_model_limits() {
        let policy: SchedulingPolicy = serde_json::from_value(json!({
            "enabled": true,
            "limits": [
                {"provider": "openai", "window_seconds": 60, "max_rps": 2,
                 "max_total_tokens": 60000},
                {"provider": "openai", "model": "reasoner", "window_seconds": 60,
                 "max_input_tokens": 30000}
            ]
        }))
        .unwrap();
        assert!(policy.validate().is_ok());
        assert_eq!(policy.limits.len(), 2);
    }

    #[test]
    fn rejects_zero_overflow_and_unknown_quota_fields() {
        let policy: SchedulingPolicy = serde_json::from_value(json!({
            "limits": [{"window_seconds": 0, "max_requests": 1}]
        }))
        .unwrap();
        assert!(policy.validate().is_err());
        let policy: SchedulingPolicy = serde_json::from_value(json!({
            "limits": [{"window_seconds": 1, "max_requests": 18446744073709551615u64}]
        }))
        .unwrap();
        assert!(policy.validate().is_err());
        assert!(
            serde_json::from_value::<SchedulingPolicy>(json!({
                "limits": [{"window_seconds": 1, "max_rps": 1, "max_rpss": 2}]
            }))
            .is_err()
        );
    }
}
