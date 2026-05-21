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
