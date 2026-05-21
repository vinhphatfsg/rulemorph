use std::path::Path;
use std::time::Instant;

use rulemorph::{RuleFile, TransformError};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use self::steps::build_step_nodes;
use super::duration::sum_rule_trace_duration_us;
use super::finalize::build_finalize_trace;
use super::mapping_ops::build_mapping_ops_with_values;

mod branch_trace;
mod step_outputs;
mod steps;

pub(in crate::endpoint_engine) struct RuleTraceNodes {
    pub(in crate::endpoint_engine) nodes: Vec<JsonValue>,
    pub(in crate::endpoint_engine) finalize: Option<JsonValue>,
    pub(in crate::endpoint_engine) pre_finalize_output: Option<JsonValue>,
    pub(in crate::endpoint_engine) duration_us: u64,
}

pub(in crate::endpoint_engine) fn build_rule_nodes_from_rule(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> RuleTraceNodes {
    let mut nodes = Vec::new();
    let mut finalize_trace: Option<JsonValue> = None;
    let mut pre_finalize_output: Option<JsonValue> = None;
    if rule.steps.is_some() {
        nodes = build_step_nodes(rule, record, context, base_dir);
    } else {
        let started = Instant::now();
        let mut out = JsonValue::Object(JsonMap::new());
        let children = build_mapping_ops_with_values(
            &rule.mappings,
            record,
            context,
            &mut out,
            rule.version,
            0,
        );
        let duration_us = started.elapsed().as_micros() as u64;
        let mut node = json!({
            "id": "step-0",
            "kind": "mapping",
            "label": "mappings",
            "status": "ok",
            "input": record,
            "output": out,
            "duration_us": duration_us,
        });
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }

    if let Some(finalize) = build_finalize_trace(rule, record, context, base_dir) {
        pre_finalize_output = finalize.pre_finalize_output;
        finalize_trace = Some(finalize.trace);
    }

    let duration_us = sum_rule_trace_duration_us(&nodes, finalize_trace.as_ref());

    RuleTraceNodes {
        nodes,
        finalize: finalize_trace,
        pre_finalize_output,
        duration_us,
    }
}

pub(super) fn transform_error_to_trace(err: &TransformError) -> JsonValue {
    json!({
        "code": format!("{:?}", err.kind),
        "message": err.message,
        "path": err.path,
    })
}
