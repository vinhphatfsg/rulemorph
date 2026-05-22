use rulemorph::RuleFile;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

pub(super) fn step_label(rule: &RuleFile, step_index: usize) -> String {
    rule.steps
        .as_deref()
        .and_then(|steps| steps.get(step_index))
        .and_then(|step| step.name.clone())
        .unwrap_or_else(|| format!("step-{}", step_index + 1))
}

pub(super) fn step_kind(rule: &RuleFile, step_index: usize) -> &'static str {
    let Some(step) = rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(step_index))
    else {
        return "step";
    };
    if step.branch.is_some() {
        "branch"
    } else if step.record_when.is_some() {
        "record_when"
    } else if step.asserts.is_some() {
        "asserts"
    } else if step.mappings.is_some() {
        "mappings"
    } else {
        "step"
    }
}

pub(super) fn build_step_node(
    step_index: usize,
    kind: &'static str,
    label: String,
    status: String,
    input: JsonValue,
    output: Option<JsonValue>,
    duration_us: u64,
    error: Option<JsonValue>,
    child_trace: Option<JsonValue>,
    meta: JsonMap<String, JsonValue>,
    children: Vec<JsonValue>,
) -> JsonValue {
    let mut node = json!({
        "id": format!("step-{}", step_index),
        "kind": kind,
        "label": label,
        "status": status,
        "input": input,
        "output": output,
        "duration_us": duration_us,
    });
    if let Some(err) = error
        && let Some(obj) = node.as_object_mut()
    {
        obj.insert("error".to_string(), err);
    }
    if let Some(trace) = child_trace
        && let Some(obj) = node.as_object_mut()
    {
        obj.insert("child_trace".to_string(), trace);
    }
    if !meta.is_empty()
        && let Some(obj) = node.as_object_mut()
    {
        obj.insert("meta".to_string(), JsonValue::Object(meta));
    }
    if !children.is_empty()
        && let Some(obj) = node.as_object_mut()
    {
        obj.insert("children".to_string(), JsonValue::Array(children));
    }
    node
}
