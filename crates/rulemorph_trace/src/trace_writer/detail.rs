use serde_json::Value as JsonValue;

use super::options::TraceDetailLevel;

pub(super) fn append_detail_reason(existing: Option<String>, reason: &str) -> Option<String> {
    match existing {
        None => Some(reason.to_string()),
        Some(current) => {
            let already_present = current
                .split(|ch| ch == ',' || ch == ';')
                .any(|item| item.trim() == reason);
            if already_present {
                Some(current)
            } else {
                Some(format!("{current},{reason}"))
            }
        }
    }
}

pub(super) fn initial_detail_reasons(reason: Option<&str>) -> Vec<String> {
    let mut detail_reason = Vec::new();
    if let Some(reason) = reason {
        for item in reason.split(|ch| ch == ',' || ch == ';') {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                continue;
            }
            if !detail_reason.iter().any(|existing| existing == trimmed) {
                detail_reason.push(trimmed.to_string());
            }
        }
    }
    detail_reason
}

pub(super) fn detail_status_for_level(detail_level: TraceDetailLevel) -> String {
    match detail_level {
        TraceDetailLevel::Full => "full".to_string(),
        TraceDetailLevel::Basic => "basic".to_string(),
        TraceDetailLevel::Off => "dropped".to_string(),
    }
}

pub(super) fn strip_trace_detail(trace: &mut JsonValue) {
    let Some(obj) = trace.as_object_mut() else {
        return;
    };
    obj.remove("records");
    obj.remove("finalize");
    obj.remove("nodes");
}
