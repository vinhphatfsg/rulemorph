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

#[test]
fn queue_full_normal_drops_new_trace() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-normal-drop");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 1,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    assert!(writer.enqueue(make_trace("trace-a", "ok")));
    assert!(!writer.enqueue(make_trace("trace-b", "ok")));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(trace_id_for_log(&guard[0].trace), "trace-a");
    assert_eq!(guard[0].priority, TracePriority::Normal);
}

#[test]
fn queue_full_high_evicts_normal_and_keeps_full() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-high-evict");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 1,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    assert!(writer.enqueue(make_trace("trace-normal", "ok")));

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Full,
        ..Default::default()
    };
    assert!(writer.enqueue_with_options(make_trace("trace-error", "error"), Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(trace_id_for_log(&guard[0].trace), "trace-error");
    assert_eq!(guard[0].priority, TracePriority::High);
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Full);
    assert!(!guard[0].downgraded);
}

#[test]
fn queue_max_bytes_downgrades_and_strips_trace() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-max-bytes");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 10,
            queue_max_bytes: 512,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-big",
        "status": "ok",
        "records": [
            { "index": 0, "payload": "x".repeat(1024) }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        }
    });

    assert!(writer.enqueue(trace));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
    assert!(guard[0].downgraded);
    assert!(guard[0].trace.get("records").is_none());
}

#[test]
fn queue_full_downgrade_sets_reason() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-reason");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 2,
            queue_max_bytes: 256,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-queue-reason",
        "status": "ok",
        "records": [
            { "index": 0, "status": "ok", "payload": "x".repeat(2048) }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        }
    });

    assert!(writer.enqueue(trace));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(
        guard[0].options.detail_reason.as_deref(),
        Some("queue_full")
    );
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
}

#[test]
fn queue_full_downgrade_appends_reason() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-reason-append");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 2,
            queue_max_bytes: 256,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-queue-reason-append",
        "status": "ok",
        "records": [
            { "index": 0, "status": "ok", "payload": "x".repeat(2048) }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        }
    });

    let options = TraceWriteOptions {
        detail_reason: Some("sampled_out".to_string()),
        ..TraceWriteOptions::default()
    };

    assert!(writer.enqueue_with_options(trace, Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    let reason = guard[0]
        .options
        .detail_reason
        .as_deref()
        .expect("detail reason");
    assert!(reason.contains("sampled_out"));
    assert!(reason.contains("queue_full"));
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
}

#[test]
fn queue_max_bytes_high_priority_downgrades_when_needed() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-high-max-bytes");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 10,
            queue_max_bytes: 512,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-error-big",
        "status": "error",
        "records": [
            { "index": 0, "payload": "x".repeat(1024) }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 0,
            "record_failed": 1
        }
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Full,
        ..Default::default()
    };
    assert!(writer.enqueue_with_options(trace, Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0].priority, TracePriority::High);
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
    assert!(guard[0].downgraded);
    assert!(guard[0].trace.get("records").is_none());
}

include!("queue_tests/masking.rs");
