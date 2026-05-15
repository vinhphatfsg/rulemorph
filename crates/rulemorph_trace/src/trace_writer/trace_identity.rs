use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value as JsonValue;
use tracing::warn;

use crate::trace_id::{sanitize_trace_id, trace_id_is_placeholder};

pub(super) struct ResolvedTraceId {
    pub(super) raw_trace_id: String,
    pub(super) trace_id: String,
}

pub(super) fn resolve_trace_id(trace: &JsonValue) -> ResolvedTraceId {
    let raw_trace_id = trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let fallback = format!("trace-{nanos}");
            warn!("trace_id missing; using generated id {}", fallback);
            fallback
        });
    let mut trace_id = sanitize_trace_id(&raw_trace_id);
    let trace_id_is_placeholder = trace_id_is_placeholder(&trace_id);
    if trace_id.is_empty() || trace_id_is_placeholder {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        trace_id = format!("trace-{nanos}");
        if trace_id_is_placeholder {
            warn!("trace_id is insufficient; using generated id {}", trace_id);
        } else {
            warn!(
                "trace_id sanitized to empty; using generated id {}",
                trace_id
            );
        }
    } else if trace_id != raw_trace_id {
        warn!("trace_id sanitized from {} to {}", raw_trace_id, trace_id);
    }

    ResolvedTraceId {
        raw_trace_id,
        trace_id,
    }
}
