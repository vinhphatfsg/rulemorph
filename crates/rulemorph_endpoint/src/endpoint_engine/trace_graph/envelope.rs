use chrono::Utc;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

pub(in crate::endpoint_engine) struct RuleTraceInput<'a> {
    pub(in crate::endpoint_engine) rule_type: &'a str,
    pub(in crate::endpoint_engine) name: String,
    pub(in crate::endpoint_engine) path: String,
    pub(in crate::endpoint_engine) version: u8,
    pub(in crate::endpoint_engine) rule_source: JsonValue,
    pub(in crate::endpoint_engine) input: JsonValue,
    pub(in crate::endpoint_engine) output: JsonValue,
    pub(in crate::endpoint_engine) nodes: Vec<JsonValue>,
    pub(in crate::endpoint_engine) finalize: Option<JsonValue>,
    pub(in crate::endpoint_engine) duration_us: u64,
    pub(in crate::endpoint_engine) status: &'a str,
}

pub(in crate::endpoint_engine) fn build_rule_trace(input: RuleTraceInput<'_>) -> JsonValue {
    let RuleTraceInput {
        rule_type,
        name,
        path,
        version,
        rule_source,
        input,
        output,
        nodes,
        finalize,
        duration_us,
        status,
    } = input;
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
    if let Some(finalize) = finalize
        && let Some(obj) = trace.as_object_mut()
    {
        obj.insert("finalize".to_string(), finalize);
    }
    trace
}
