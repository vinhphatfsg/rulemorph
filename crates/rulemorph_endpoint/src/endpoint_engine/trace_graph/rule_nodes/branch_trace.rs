use std::path::Path;

use rulemorph::transform_record_with_base_dir;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use super::build_rule_nodes_from_rule;
use crate::endpoint_engine::rule_loader::{RuleKind, load_rule_kind, yaml_source_to_json};
use crate::endpoint_engine::trace_graph::build_rule_trace;
use crate::endpoint_engine::{
    empty_object, resolve_rule_path, rule_display_name, rule_ref_from_path, rule_ref_from_rule,
};

pub(super) fn apply_branch_trace_meta(
    then_path: &str,
    else_path: Option<&str>,
    branch_taken: &str,
    base_dir: &Path,
    step_input: &JsonValue,
    context: Option<&JsonValue>,
    meta: &mut JsonMap<String, JsonValue>,
) -> Option<JsonValue> {
    let mut refs = Vec::new();
    let mut labels = Vec::new();
    let then_ref = rule_ref_from_rule(base_dir, then_path);
    refs.push(then_ref.clone());
    labels.push("branch: then".to_string());
    let else_ref = else_path.map(|other| rule_ref_from_rule(base_dir, other));
    if let Some(other_ref) = else_ref.as_ref() {
        refs.push(other_ref.clone());
        labels.push("branch: else".to_string());
    }

    meta.insert(
        "branch_taken".to_string(),
        JsonValue::String(branch_taken.to_string()),
    );
    meta.insert(
        "rule_refs".to_string(),
        JsonValue::Array(refs.iter().cloned().map(JsonValue::String).collect()),
    );
    meta.insert(
        "rule_ref_labels".to_string(),
        JsonValue::Array(labels.iter().cloned().map(JsonValue::String).collect()),
    );

    let taken_ref = match branch_taken {
        "then" => Some((then_path, then_ref)),
        "else" => else_path.and_then(|path| else_ref.map(|label| (path, label))),
        _ => None,
    };
    if let Some((target_path, ref_label)) = taken_ref {
        meta.insert("rule_ref".to_string(), JsonValue::String(ref_label.clone()));
        meta.insert(
            "rule_ref_label".to_string(),
            JsonValue::String(format!("branch: {}", branch_taken)),
        );
        let resolved = resolve_rule_path(base_dir, target_path);
        if let Ok(RuleKind::Normal(loaded)) = load_rule_kind(&resolved) {
            let rule_source = std::fs::read_to_string(&resolved)
                .ok()
                .and_then(|source| yaml_source_to_json(&source))
                .unwrap_or_else(|| json!({}));
            let child_rule_trace =
                build_rule_nodes_from_rule(&loaded.rule, step_input, context, &loaded.base_dir);
            let child_duration_us = child_rule_trace.duration_us;
            let child_output =
                transform_record_with_base_dir(&loaded.rule, step_input, context, &loaded.base_dir)
                    .ok()
                    .and_then(|value| value)
                    .unwrap_or_else(empty_object);
            let trace_output = child_rule_trace
                .pre_finalize_output
                .clone()
                .unwrap_or_else(|| child_output.clone());
            return Some(build_rule_trace(
                "normal",
                rule_display_name(&resolved),
                rule_ref_from_path(base_dir, &resolved),
                loaded.rule.version,
                rule_source,
                step_input.clone(),
                trace_output,
                child_rule_trace.nodes,
                child_rule_trace.finalize,
                child_duration_us,
                "ok",
            ));
        }
    }
    None
}
