#[test]
fn trace_max_snapshot_bytes_marks_incomplete_without_breaking_parent_ids() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "payload"
    source: "payload"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut options = TransformTraceOptions::raw();
    options.max_snapshot_bytes = Some(8);

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"payload":"this string is deliberately oversized"}]"#),
        None,
        &options,
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([{ "payload": "this string is deliberately oversized" }])
    );
    assert!(!traced.trace.complete);
    assert_eq!(
        traced
            .trace
            .truncation
            .as_ref()
            .map(|truncation| truncation.reason.as_str()),
        Some("max_snapshot_bytes")
    );
    assert_parent_ids_point_to_emitted_events(&traced.trace);

    let events = iter_trace_events(&traced.trace);
    let snapshots = events
        .iter()
        .flat_map(|event| event.inputs.iter().chain(event.output.iter()));
    assert!(snapshots.into_iter().any(|snapshot| {
        snapshot.visibility.as_deref() == Some("truncated")
            && snapshot.redaction_reason.as_deref() == Some("max_snapshot_bytes")
            && !snapshot.contains_raw_value
            && snapshot.value.is_none()
    }));
}

#[test]
fn trace_max_snapshot_bytes_counts_bytes_before_first_truncated_snapshot() {
    let yaml = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "input.id"
  - target: "big"
    source: "input.big"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut options = TransformTraceOptions::raw();
    options.max_snapshot_bytes = Some(40);

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"id":"a","big":"ok"},{"id":"b","big":"abcdefghijklmnopqrstuvwxyz"}]"#),
        None,
        &options,
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([
            { "id": "a", "big": "ok" },
            { "id": "b", "big": "abcdefghijklmnopqrstuvwxyz" }
        ])
    );
    let truncation = traced.trace.truncation.as_ref().expect("truncation");
    assert_eq!(truncation.reason, "max_snapshot_bytes");

    let events = iter_trace_events(&traced.trace);
    let truncated_event = events
        .get(truncation.emitted_events)
        .expect("truncated event should be emitted");
    let truncated_event_bytes = serde_json::to_value(truncated_event)
        .ok()
        .and_then(|value| serde_json::to_vec(&value).ok())
        .map(|bytes| bytes.len())
        .expect("event should serialize");

    assert!(
        truncation.emitted_bytes > truncated_event_bytes,
        "snapshot truncation should include bytes from prior emitted events: truncation={:?} truncated_event_bytes={}",
        truncation,
        truncated_event_bytes
    );
}

#[test]
fn trace_max_trace_bytes_freezes_with_truncation_reason() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut options = TransformTraceOptions::raw();
    options.max_trace_bytes = Some(1);

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &options,
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    assert!(!traced.trace.complete);
    let truncation = traced.trace.truncation.as_ref().expect("truncation");
    assert_eq!(truncation.reason, "max_trace_bytes");
    assert_eq!(
        truncation.emitted_events,
        iter_trace_events(&traced.trace).len()
    );
    assert_parent_ids_point_to_emitted_events(&traced.trace);
}
