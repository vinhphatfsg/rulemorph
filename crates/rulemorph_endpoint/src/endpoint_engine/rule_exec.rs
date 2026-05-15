use std::path::Path;

use anyhow::Result;
use rulemorph::transform_record_with_base_dir;
use serde_json::{Value as JsonValue, json};

use super::config::RequestContext;
use super::error::EndpointError;
use super::rule_ref::{resolve_rule_path, rule_display_name, rule_ref_from_path};
use super::trace_graph::{
    build_network_nodes_with_timing, build_rule_nodes_from_rule, build_rule_trace,
};
use super::{EndpointEngine, RuleKind, load_rule_kind, yaml_source_to_json};

pub(super) struct RuleExecution {
    pub(super) output: JsonValue,
    pub(super) child_trace: Option<JsonValue>,
}

pub(super) struct RuleExecutionError {
    pub(super) error: EndpointError,
    pub(super) child_trace: Option<JsonValue>,
}

impl RuleExecutionError {
    fn new(error: EndpointError) -> Self {
        Self {
            error,
            child_trace: None,
        }
    }

    fn with_child_trace(mut self, trace: Option<JsonValue>) -> Self {
        self.child_trace = trace;
        self
    }
}

impl From<EndpointError> for RuleExecutionError {
    fn from(error: EndpointError) -> Self {
        Self::new(error)
    }
}

impl EndpointEngine {
    pub(super) async fn execute_rule(
        &self,
        rule_path: &str,
        input: &JsonValue,
        context: Option<&JsonValue>,
        base_dir: &Path,
        request_context: Option<&RequestContext>,
    ) -> Result<RuleExecution, RuleExecutionError> {
        let resolved = resolve_rule_path(base_dir, rule_path);
        let rule_source = std::fs::read_to_string(&resolved)
            .ok()
            .and_then(|source| yaml_source_to_json(&source))
            .unwrap_or_else(|| json!({}));
        let rule_ref = rule_ref_from_path(base_dir, &resolved);
        match load_rule_kind(&resolved).map_err(|err| {
            RuleExecutionError::new(
                EndpointError::invalid(err.to_string()).with_path(resolved.clone()),
            )
        })? {
            RuleKind::Normal(rule) => {
                let rule_trace =
                    build_rule_nodes_from_rule(&rule.rule, input, context, &rule.base_dir);
                let duration_us = rule_trace.duration_us;
                let output_result =
                    transform_record_with_base_dir(&rule.rule, input, context, &rule.base_dir);
                let output = match output_result {
                    Ok(Some(output)) => output,
                    Ok(None) => {
                        let record_output = rule_trace
                            .pre_finalize_output
                            .clone()
                            .unwrap_or(JsonValue::Null);
                        let child_trace = build_rule_trace(
                            "normal",
                            rule_display_name(&resolved),
                            rule_ref,
                            rule.rule.version,
                            rule_source,
                            input.clone(),
                            record_output,
                            rule_trace.nodes,
                            rule_trace.finalize,
                            duration_us,
                            "error",
                        );
                        return Err(RuleExecutionError::new(
                            EndpointError::invalid(format!(
                                "record excluded by rule: {}",
                                rule_display_name(&resolved)
                            ))
                            .with_path(resolved.clone()),
                        )
                        .with_child_trace(Some(child_trace)));
                    }
                    Err(err) => {
                        let record_output = rule_trace
                            .pre_finalize_output
                            .clone()
                            .unwrap_or(JsonValue::Null);
                        let child_trace = build_rule_trace(
                            "normal",
                            rule_display_name(&resolved),
                            rule_ref,
                            rule.rule.version,
                            rule_source,
                            input.clone(),
                            record_output,
                            rule_trace.nodes,
                            rule_trace.finalize,
                            duration_us,
                            "error",
                        );
                        return Err(RuleExecutionError::new(
                            EndpointError::from_transform(err).with_path(resolved.clone()),
                        )
                        .with_child_trace(Some(child_trace)));
                    }
                };
                let record_output = rule_trace
                    .pre_finalize_output
                    .clone()
                    .unwrap_or_else(|| output.clone());
                let child_trace = build_rule_trace(
                    "normal",
                    rule_display_name(&resolved),
                    rule_ref,
                    rule.rule.version,
                    rule_source,
                    input.clone(),
                    record_output,
                    rule_trace.nodes,
                    rule_trace.finalize,
                    duration_us,
                    "ok",
                );
                Ok(RuleExecution {
                    output,
                    child_trace: Some(child_trace),
                })
            }
            RuleKind::Network(rule) => {
                let execution = self
                    .execute_network(&rule, input, context, request_context)
                    .await
                    .map_err(|err| RuleExecutionError::new(err.with_path(resolved.clone())))?;
                let nodes = build_network_nodes_with_timing(&rule, &execution);
                let child_trace = build_rule_trace(
                    "network",
                    rule_display_name(&resolved),
                    rule_ref,
                    2,
                    rule_source,
                    input.clone(),
                    execution.output.clone(),
                    nodes,
                    None,
                    execution.total_us,
                    "ok",
                );
                Ok(RuleExecution {
                    output: execution.output,
                    child_trace: Some(child_trace),
                })
            }
        }
    }
}
