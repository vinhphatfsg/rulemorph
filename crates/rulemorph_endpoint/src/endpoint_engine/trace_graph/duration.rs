use serde_json::Value as JsonValue;

fn sum_node_duration_us(nodes: &[JsonValue]) -> u64 {
    nodes
        .iter()
        .filter_map(|node| node.get("duration_us").and_then(|value| value.as_u64()))
        .sum()
}

pub(in crate::endpoint_engine) fn sum_rule_trace_duration_us(
    nodes: &[JsonValue],
    finalize: Option<&JsonValue>,
) -> u64 {
    sum_node_duration_us(nodes).saturating_add(
        finalize
            .and_then(|trace| trace.get("duration_us"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
    )
}
