#[test]
fn trace_records_source_default_and_output_write_with_raw_values() {
    let yaml = r#"
version: 2
input:
  format: json
record_when:
  eq: ["@input.active", true]
mappings:
  - target: "profile.name"
    source: "name"
  - target: "profile.nickname"
    source: "nickname"
    default: "anonymous"
  - target: "profile.skip"
    source: "skip"
    when:
      eq: ["@input.include_skip", true]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"active":true,"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    let events = &traced.trace.records[0].events;
    let kinds = events
        .iter()
        .map(|event| serde_json::to_value(&event.kind).unwrap())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&json!("record_when_start")));
    assert!(kinds.contains(&json!("record_when_end")));
    assert!(kinds.contains(&json!("source_read")));
    assert!(kinds.contains(&json!("default_applied")));
    assert!(kinds.contains(&json!("mapping_when_start")));
    assert!(kinds.contains(&json!("mapping_when_end")));
    assert!(kinds.contains(&json!("mapping_decision")));
    assert!(kinds.contains(&json!("output_write")));

    let raw_values = serde_json::to_string(&traced.trace).expect("serialize trace");
    assert!(raw_values.contains("alice"));
    assert!(raw_values.contains("anonymous"));
}
