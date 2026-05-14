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

#[test]
fn enqueue_basic_strips_and_masks_rule_source() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-basic-mask");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-basic-mask",
        "status": "ok",
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ],
        "rule_source": {
            "token": "secret",
            "headers": {
                "Authorization": "Bearer 123"
            }
        }
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Basic,
        ..Default::default()
    };

    assert!(writer.enqueue_with_options(trace, Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
    assert!(guard[0].trace.get("records").is_none());
    let rule_source = guard[0]
        .trace
        .get("rule_source")
        .and_then(|value| value.as_object())
        .expect("rule_source should exist");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    let headers = rule_source
        .get("headers")
        .and_then(|value| value.as_object())
        .expect("headers should exist");
    assert_eq!(
        headers
            .get("Authorization")
            .and_then(|value| value.as_str()),
        Some("[masked]")
    );
}

#[test]
fn enqueue_basic_masks_summary_fields() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-basic-summary-mask");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-basic-summary-mask",
        "status": "ok",
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ],
        "summary": {
            "input": { "password": "secret", "ok": 1 },
            "output": { "token": "secret" }
        }
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Basic,
        ..Default::default()
    };

    assert!(writer.enqueue_with_options(trace, Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert!(guard[0].trace.get("records").is_none());
    let summary = guard[0]
        .trace
        .get("summary")
        .and_then(|value| value.as_object())
        .expect("summary should exist");
    let summary_input = summary
        .get("input")
        .and_then(|value| value.as_object())
        .expect("summary input");
    let summary_output = summary
        .get("output")
        .and_then(|value| value.as_object())
        .expect("summary output");
    assert_eq!(
        summary_input
            .get("password")
            .and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        summary_output.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
}

#[test]
fn enqueue_off_masks_summary_fields() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-off-summary-mask");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    let trace = json!({
        "trace_id": "trace-off-summary-mask",
        "status": "ok",
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ],
        "summary": {
            "input": { "password": "secret", "ok": 1 },
            "output": { "token": "secret" }
        },
        "rule_source": {
            "token": "secret"
        }
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Off,
        ..Default::default()
    };

    assert!(writer.enqueue_with_options(trace, Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert!(guard[0].trace.get("records").is_none());
    let summary = guard[0]
        .trace
        .get("summary")
        .and_then(|value| value.as_object())
        .expect("summary should exist");
    let summary_input = summary
        .get("input")
        .and_then(|value| value.as_object())
        .expect("summary input");
    let summary_output = summary
        .get("output")
        .and_then(|value| value.as_object())
        .expect("summary output");
    assert_eq!(
        summary_input
            .get("password")
            .and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        summary_output.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    let rule_source = guard[0]
        .trace
        .get("rule_source")
        .and_then(|value| value.as_object())
        .expect("rule_source should exist");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
}
