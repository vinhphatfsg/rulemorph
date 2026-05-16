use chrono::Utc;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

pub(in crate::endpoint_engine) fn build_rule_trace(
    rule_type: &str,
    name: String,
    path: String,
    version: u8,
    rule_source: JsonValue,
    input: JsonValue,
    output: JsonValue,
    nodes: Vec<JsonValue>,
    finalize: Option<JsonValue>,
    duration_us: u64,
    status: &str,
) -> JsonValue {
    let trace_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let record = json!({
        "index": 0,
        "status": status,
        "duration_us": duration_us,
        "input": input,
        "output": output,
        "nodes": nodes,
    });
    let mut trace = json!({
        "trace_id": trace_id,
        "timestamp": now.to_rfc3339(),
        "rule": {
            "type": rule_type,
            "name": name,
            "path": path,
            "version": version
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
    });
    if let Some(finalize) = finalize {
        if let Some(obj) = trace.as_object_mut() {
            obj.insert("finalize".to_string(), finalize);
        }
    }
    trace
}
