use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{config::Mode, error::ProxyError};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingPolicy {
    pub max_direct_cost_usd: f64,
    pub max_direct_premium_usd: f64,
    pub models: BTreeMap<String, ModelPricing>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPricing {
    pub input_usd_per_million: f64,
    pub output_usd_per_million: f64,
    pub batch_input_usd_per_million: f64,
    pub batch_output_usd_per_million: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RouteDecision {
    pub mode: Mode,
    pub reason: &'static str,
    pub estimated_input_tokens: Option<u64>,
    pub output_token_limit: Option<u64>,
    pub estimated_direct_cost_usd: Option<f64>,
    pub estimated_batch_cost_usd: Option<f64>,
    pub estimated_direct_premium_usd: Option<f64>,
}

impl RouteDecision {
    pub fn new(mode: Mode, reason: &'static str) -> Self {
        Self {
            mode,
            reason,
            estimated_input_tokens: None,
            output_token_limit: None,
            estimated_direct_cost_usd: None,
            estimated_batch_cost_usd: None,
            estimated_direct_premium_usd: None,
        }
    }
}

impl RoutingPolicy {
    pub fn load(path: Option<&Path>) -> Result<Self, ProxyError> {
        let policy: Self = match path {
            Some(path) => serde_json::from_slice(&std::fs::read(path).map_err(|error| {
                ProxyError::BadRequest(format!(
                    "cannot read routing policy {}: {error}",
                    path.display()
                ))
            })?)?,
            None => Self::default(),
        };
        let mut amounts = vec![policy.max_direct_cost_usd, policy.max_direct_premium_usd];
        for pricing in policy.models.values() {
            amounts.extend([
                pricing.input_usd_per_million,
                pricing.output_usd_per_million,
                pricing.batch_input_usd_per_million,
                pricing.batch_output_usd_per_million,
            ]);
        }
        if amounts
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(ProxyError::BadRequest(
                "routing prices and allowances must be finite and nonnegative".into(),
            ));
        }
        Ok(policy)
    }

    pub fn decide(&self, body: &Value) -> RouteDecision {
        let mut decision = RouteDecision::new(Mode::Batch, "unknown_model_pricing");
        let Some(pricing) = body
            .get("model")
            .and_then(Value::as_str)
            .and_then(|model| self.models.get(model))
        else {
            return decision;
        };
        if matches!(
            body.pointer("/reasoning/effort").and_then(Value::as_str),
            Some("medium" | "high" | "xhigh" | "max")
        ) {
            decision.reason = "reasoning_workload";
            return decision;
        }
        if contains_non_text_input(body) {
            decision.reason = "unpriced_modality_or_hosted_tool";
            return decision;
        }
        let Some(output_tokens) = body
            .get("max_output_tokens")
            .and_then(Value::as_u64)
            .filter(|limit| *limit > 0)
        else {
            decision.reason = "unbounded_output";
            return decision;
        };
        let input_tokens = estimate_input_tokens(body);
        let direct = (input_tokens as f64 * pricing.input_usd_per_million
            + output_tokens as f64 * pricing.output_usd_per_million)
            / 1_000_000.0;
        let batch = (input_tokens as f64 * pricing.batch_input_usd_per_million
            + output_tokens as f64 * pricing.batch_output_usd_per_million)
            / 1_000_000.0;
        if !direct.is_finite() || !batch.is_finite() {
            decision.reason = "cost_estimate_overflow";
            return decision;
        }
        let premium = (direct - batch).max(0.0);
        decision.estimated_input_tokens = Some(input_tokens);
        decision.output_token_limit = Some(output_tokens);
        decision.estimated_direct_cost_usd = Some(direct);
        decision.estimated_batch_cost_usd = Some(batch);
        decision.estimated_direct_premium_usd = Some(premium);
        if direct <= self.max_direct_cost_usd && premium <= self.max_direct_premium_usd {
            decision.mode = Mode::Direct;
            decision.reason = "within_direct_allowance";
        } else {
            decision.reason = "batch_savings";
        }
        decision
    }
}

fn estimate_input_tokens(body: &Value) -> u64 {
    let bytes: usize = ["instructions", "input", "tools", "text"]
        .iter()
        .filter_map(|key| body.get(key))
        .map(|value| value.to_string().len())
        .sum();
    (bytes as u64).div_ceil(3).saturating_add(32)
}

fn contains_non_text_input(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(contains_non_text_input),
        Value::Object(object) => {
            object
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| {
                    !matches!(
                        kind,
                        "message"
                            | "input_text"
                            | "output_text"
                            | "function"
                            | "function_call"
                            | "function_call_output"
                            | "text"
                            | "json_schema"
                            | "json_object"
                            | "object"
                            | "array"
                            | "string"
                            | "number"
                            | "integer"
                            | "boolean"
                            | "null"
                    )
                })
                || object.values().any(contains_non_text_input)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy() -> RoutingPolicy {
        serde_json::from_value(json!({
            "max_direct_cost_usd": 0.001,
            "max_direct_premium_usd": 0.0005,
            "models": {"test": {
                "input_usd_per_million": 1.0, "output_usd_per_million": 4.0,
                "batch_input_usd_per_million": 0.5, "batch_output_usd_per_million": 2.0
            }}
        }))
        .unwrap()
    }

    #[test]
    fn routes_by_full_context_and_output_limit() {
        let mut body = json!({"model": "test", "input": "next", "max_output_tokens": 64});
        assert_eq!(policy().decide(&body).mode, Mode::Direct);
        body["input"] = json!("a".repeat(30_000));
        assert_eq!(policy().decide(&body).mode, Mode::Batch);
        body["input"] = json!("next");
        body["max_output_tokens"] = json!(10_000);
        assert_eq!(policy().decide(&body).mode, Mode::Batch);
    }

    #[test]
    fn uncertainty_and_reasoning_favor_batch() {
        for extra in [
            json!({}),
            json!({"max_output_tokens": 10, "reasoning": {"effort": "high"}}),
            json!({"max_output_tokens": 10, "tools": [{"type": "web_search"}]}),
        ] {
            let mut body = json!({"model": "test", "input": "next"});
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert_eq!(policy().decide(&body).mode, Mode::Batch);
        }
        assert_eq!(
            RoutingPolicy::default()
                .decide(&json!({"model":"unknown"}))
                .mode,
            Mode::Batch
        );
    }

    #[test]
    fn zero_premium_means_do_not_pay_for_latency() {
        let mut policy = policy();
        policy.max_direct_premium_usd = 0.0;
        assert_eq!(
            policy
                .decide(&json!({"model":"test", "input":"next", "max_output_tokens":10}))
                .mode,
            Mode::Batch
        );
    }

    #[test]
    fn local_function_schemas_are_priced_as_text() {
        let body = json!({"model": "test", "max_output_tokens": 10,
            "tools": [{"type": "function", "name": "read", "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}]});
        assert_eq!(policy().decide(&body).mode, Mode::Direct);
    }

    #[test]
    fn rejects_invalid_policy_instead_of_ignoring_misspelled_limits() {
        let file = tempfile::NamedTempFile::new().unwrap();
        for policy in [
            r#"{"max_direct_cost_usd": -1}"#,
            r#"{"max_direct_cost_us": 1}"#,
        ] {
            std::fs::write(file.path(), policy).unwrap();
            assert!(RoutingPolicy::load(Some(file.path())).is_err());
        }
    }
}
