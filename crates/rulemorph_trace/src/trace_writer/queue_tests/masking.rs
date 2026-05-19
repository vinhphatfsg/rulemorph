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
