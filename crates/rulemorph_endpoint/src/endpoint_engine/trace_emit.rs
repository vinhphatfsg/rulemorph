use std::path::Path;

use anyhow::Result;
use axum::http::Method;
use chrono::Utc;
use serde_json::{Value as JsonValue, json};
use tracing::warn;
use uuid::Uuid;

use super::EndpointEngine;
use super::endpoint_rule::CompiledStep;
use super::error::EndpointError;
use super::rule_ref::{rule_ref_from_path, rule_ref_from_rule, safe_rule_ref_from_path};

pub(super) struct EndpointTraceInput<'a> {
    pub(super) method: &'a Method,
    pub(super) path: &'a str,
    pub(super) input: JsonValue,
    pub(super) output: JsonValue,
    pub(super) status: String,
    pub(super) error: Option<JsonValue>,
    pub(super) nodes: Vec<JsonValue>,
    pub(super) duration_us: u64,
}

pub(super) struct EndpointStepTraceInput<'a> {
    pub(super) step_index: usize,
    pub(super) step: &'a CompiledStep,
    pub(super) status: &'a str,
    pub(super) input: JsonValue,
    pub(super) output: Option<JsonValue>,
    pub(super) error: Option<EndpointError>,
    pub(super) duration_us: u64,
    pub(super) child_trace: Option<JsonValue>,
}

impl EndpointEngine {
    pub(super) fn build_trace(&self, input: EndpointTraceInput<'_>) -> JsonValue {
        let EndpointTraceInput {
            method,
            path,
            input,
            output,
            status,
            error,
            nodes,
            duration_us,
        } = input;
        let trace_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let rule_path = rule_ref_from_path(
            &self.endpoint_rule.base_dir,
            &self.endpoint_rule.source_path,
        );
        let rule_source = self.raw_rule_source.clone();
        let record = json!({
            "index": 0,
            "status": status,
            "duration_us": duration_us,
            "input": input,
            "output": output,
            "nodes": nodes,
            "error": error
        });
        json!({
            "trace_id": trace_id,
            "status": status,
            "timestamp": now.to_rfc3339(),
            "rule": {
                "type": "endpoint",
                "name": format!("{} {}", method.as_str(), path),
                "path": rule_path,
                "version": 2
            },
            "input_format": "json",
            "rule_source": rule_source,
            "records": [record],
            "summary": {
                "record_total": 1,
                "record_success": if status == "ok" { 1 } else { 0 },
                "record_failed": if status == "ok" { 0 } else { 1 },
                "duration_us": duration_us
            }
        })
    }

    pub(super) fn build_step_trace(&self, input: EndpointStepTraceInput<'_>) -> JsonValue {
        let EndpointStepTraceInput {
            step_index,
            step,
            status,
            input,
            output,
            error,
            duration_us,
            child_trace,
        } = input;
        let label = step_label(&step.rule);
        let rule_ref = rule_ref_from_rule(&self.endpoint_rule.base_dir, &step.rule);
        let mut node = json!({
            "id": format!("step-{}", step_index),
            "kind": "endpoint",
            "label": label,
            "status": status,
            "input": input,
            "output": output,
            "duration_us": duration_us,
            "meta": {
                "rule_ref": rule_ref,
                "step_index": step_index
            }
        });
        if let Some(err) = error
            && let Some(obj) = node.as_object_mut()
        {
            obj.insert("error".to_string(), self.endpoint_error_to_trace(&err));
        }
        if let Some(child_trace) = child_trace
            && let Some(obj) = node.as_object_mut()
        {
            obj.insert("child_trace".to_string(), child_trace);
        }
        node
    }

    pub(super) fn endpoint_error_to_trace(&self, err: &EndpointError) -> JsonValue {
        let path = err
            .path
            .as_ref()
            .and_then(|path| safe_rule_ref_from_path(&self.endpoint_rule.base_dir, path));
        json!({
            "code": format!("{:?}", err.kind),
            "message": err.message,
            "path": path
        })
    }

    pub(super) async fn write_trace(&self, trace: JsonValue) -> Result<()> {
        let trace_id = trace
            .get("trace_id")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        if !self.trace_writer.enqueue(trace) {
            warn!("trace queue full; dropped trace {}", trace_id);
        }
        Ok(())
    }
}

fn step_label(rule: &str) -> String {
    let path = Path::new(rule);
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(rule)
        .to_string()
}
