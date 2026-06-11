use std::path::Path;

use rulemorph::transform_record_with_base_dir;
use rulemorph::v2_eval::EvalValue;
use serde_json::{Value as JsonValue, json};

use super::*;
use crate::endpoint_engine::expr::{apply_mappings_via_rule, eval_expr_value};
use crate::endpoint_engine::trace_graph::{
    RuleTraceInput, build_rule_nodes_from_rule, build_rule_trace,
};

impl EndpointEngine {
    pub(in crate::endpoint_engine) fn build_network_body(
        &self,
        rule: &CompiledNetworkRule,
        input: &JsonValue,
        context: Option<&JsonValue>,
    ) -> Result<Option<JsonValue>, EndpointError> {
        if let Some(body_expr) = &rule.body {
            let value = eval_expr_value(body_expr, input, context)
                .map_err(|err| EndpointError::invalid(err.to_string()))?;
            return Ok(match value {
                EvalValue::Missing => None,
                EvalValue::Value(val) => Some(val),
            });
        }
        if let Some(mappings) = &rule.body_map {
            let output = apply_mappings_via_rule(mappings, input, context)
                .map_err(EndpointError::from_transform)?
                .unwrap_or_else(empty_object);
            return Ok(Some(output));
        }
        if let Some(body_rule) = &rule.body_rule {
            let output = transform_record_with_base_dir(
                &body_rule.rule,
                input,
                context,
                &body_rule.base_dir,
            )
            .map_err(EndpointError::from_transform)?;
            return Ok(output);
        }
        Ok(None)
    }

    pub(super) fn build_body_rule_trace(
        rule: &CompiledNetworkRule,
        input: &JsonValue,
        context: Option<&JsonValue>,
        output: Option<&JsonValue>,
    ) -> Option<JsonValue> {
        let body_rule = rule.body_rule.as_ref()?;
        let rule_ref = rule
            .body_rule_ref
            .clone()
            .unwrap_or_else(|| "body_rule".to_string());
        let name = Path::new(&rule_ref)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("body_rule")
            .to_string();
        let rule_trace =
            build_rule_nodes_from_rule(&body_rule.rule, input, context, &body_rule.base_dir);
        let duration_us = rule_trace.duration_us;
        let output_value = rule_trace
            .pre_finalize_output
            .clone()
            .or_else(|| output.cloned())
            .unwrap_or(JsonValue::Null);
        Some(build_rule_trace(RuleTraceInput {
            rule_type: "normal",
            name,
            path: rule_ref,
            version: body_rule.rule.version,
            rule_source: json!({}),
            input: input.clone(),
            output: output_value,
            nodes: rule_trace.nodes,
            finalize: rule_trace.finalize,
            duration_us,
            status: "ok",
        }))
    }
}
