use serde_json::Value;

pub(super) fn attach_empty_records(trace: &mut Value) {
    if let Some(obj) = trace.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(Vec::new()));
    }
}

pub(super) fn strip_non_full_detail(trace: &mut Value) {
    if let Some(obj) = trace.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(Vec::new()));
        obj.remove("finalize");
        if let Some(detail_obj) = obj
            .get_mut("detail")
            .and_then(|value| value.as_object_mut())
        {
            strip_detail_chunks(detail_obj);
        }
    }
}

pub(super) fn downgrade_chunk_error_detail(
    trace: &mut Value,
    mut detail_status: String,
    mut detail_reason: Vec<String>,
    size_exceeded: bool,
    budget_exceeded: bool,
) {
    if detail_status == "full" {
        detail_status = "basic".to_string();
    }
    if size_exceeded {
        push_reason_once(&mut detail_reason, "chunk_too_large");
    }
    if budget_exceeded {
        push_reason_once(&mut detail_reason, "budget_exceeded");
    }
    push_reason_once(&mut detail_reason, "chunk_error");

    if let Some(obj) = trace.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(Vec::new()));
        obj.remove("finalize");
        if let Some(detail_obj) = obj
            .get_mut("detail")
            .and_then(|value| value.as_object_mut())
        {
            detail_obj.insert("status".to_string(), Value::String(detail_status));
            detail_obj.insert(
                "reason".to_string(),
                Value::Array(detail_reason.into_iter().map(Value::String).collect()),
            );
            strip_detail_chunks(detail_obj);
        }
    }
}

fn strip_detail_chunks(obj: &mut serde_json::Map<String, Value>) {
    obj.insert("records".to_string(), Value::Array(Vec::new()));
    obj.insert("nodes".to_string(), Value::Array(Vec::new()));
    obj.remove("finalize");
}

fn push_reason_once(reasons: &mut Vec<String>, reason: &str) {
    if !reasons.iter().any(|current| current == reason) {
        reasons.push(reason.to_string());
    }
}
