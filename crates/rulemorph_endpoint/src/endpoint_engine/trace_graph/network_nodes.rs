use serde_json::{Map as JsonMap, Value as JsonValue, json};

use super::mapping_ops::{MappingOpsInput, build_mapping_ops_with_values};
use crate::endpoint_engine::{CompiledNetworkRule, NetworkExecution};

pub(in crate::endpoint_engine) fn build_network_nodes_with_timing(
    rule: &CompiledNetworkRule,
    timing: &NetworkExecution,
) -> Vec<JsonValue> {
    let mut children = Vec::new();
    let mut request_args = JsonMap::new();
    request_args.insert(
        "method".to_string(),
        JsonValue::String(rule.request.method.to_string()),
    );
    request_args.insert(
        "url".to_string(),
        JsonValue::String(format!("{:?}", rule.request.url)),
    );
    if !rule.request.headers.is_empty() {
        let mut headers = JsonMap::new();
        for (key, expr) in &rule.request.headers {
            headers.insert(key.to_string(), JsonValue::String(format!("{:?}", expr)));
        }
        request_args.insert("headers".to_string(), JsonValue::Object(headers));
    }
    children.push(json!({
        "id": "op-request",
        "kind": "op",
        "label": "request",
        "status": "ok",
        "duration_us": timing.request_us,
        "meta": { "op": "request" },
        "args": JsonValue::Object(request_args)
    }));

    if let Some(body) = &rule.body {
        children.push(json!({
            "id": "op-body",
            "kind": "op",
            "label": "body",
            "status": "ok",
            "meta": { "op": "body" },
            "args": { "expr": format!("{:?}", body) }
        }));
    }
    if let Some(body_map) = &rule.body_map {
        let mut out = JsonValue::Object(JsonMap::new());
        let empty = JsonValue::Object(JsonMap::new());
        let ops = build_mapping_ops_with_values(MappingOpsInput {
            rule: None,
            mappings: body_map,
            record: &empty,
            context: None,
            out: &mut out,
            rule_version: 2,
            step_index: 0,
            trace_ctx: None,
        });
        children.extend(ops);
    }
    if rule.body_rule.is_some() {
        children.push(json!({
            "id": "op-body-rule",
            "kind": "op",
            "label": "body_rule",
            "status": "ok",
            "meta": { "op": "body_rule" }
        }));
    }
    if let Some(select) = &rule.select {
        children.push(json!({
            "id": "op-select",
            "kind": "op",
            "label": "select",
            "status": "ok",
            "meta": { "op": "select" },
            "args": { "path": select }
        }));
    }
    if let Some(retry) = &rule.retry {
        children.push(json!({
            "id": "op-retry",
            "kind": "op",
            "label": "retry",
            "status": "ok",
            "meta": { "op": "retry" },
            "args": {
                "max": retry.max,
                "backoff": format!("{:?}", retry.backoff),
                "initial_delay_ms": retry.initial_delay.as_millis()
            }
        }));
    }

    let mut node = json!({
        "id": "step-0",
        "kind": "network",
        "label": "request",
        "status": "ok",
        "duration_us": timing.total_us,
    });
    if let Some(rule_ref) = rule.body_rule_ref.as_ref()
        && let Some(obj) = node.as_object_mut()
    {
        obj.insert(
            "meta".to_string(),
            json!({
                "rule_ref": rule_ref,
                "rule_ref_label": "body_rule"
            }),
        );
    }
    if let Some(trace) = timing.body_rule_trace.as_ref()
        && let Some(obj) = node.as_object_mut()
    {
        obj.insert("child_trace".to_string(), trace.clone());
    }
    if let Some(obj) = node.as_object_mut() {
        obj.insert("children".to_string(), JsonValue::Array(children));
    }
    vec![node]
}
