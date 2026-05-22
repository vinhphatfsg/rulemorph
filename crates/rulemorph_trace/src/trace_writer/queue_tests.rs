use super::options::{DEFAULT_TRACE_QUEUE_CAPACITY, DEFAULT_TRACE_QUEUE_MAX_BYTES};
use super::queue::trace_id_for_log;
use super::sampling::TracePriority;
use super::*;
use serde_json::json;

fn make_trace(trace_id: &str, status: &str) -> JsonValue {
    json!({
        "trace_id": trace_id,
        "status": status,
        "records": [
            { "index": 0, "status": status }
        ],
        "summary": {
            "record_total": 1,
            "record_success": if status == "ok" { 1 } else { 0 },
            "record_failed": if status == "ok" { 0 } else { 1 }
        }
    })
}

include!("queue_tests/capacity.rs");
include!("queue_tests/bytes.rs");
include!("queue_tests/masking.rs");
