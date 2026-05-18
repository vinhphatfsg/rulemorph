#[test]
fn trace_value_raw_mode_preserves_missing_null_and_empty_string() {
    let options = TransformTraceOptions::raw();

    let missing = TraceValueSnapshot::missing(&options, None);
    let null = TraceValueSnapshot::from_json(&json!(null), &options, None);
    let empty = TraceValueSnapshot::from_json(&json!(""), &options, None);

    assert_eq!(missing.state, TraceValueState::Missing);
    assert_eq!(missing.value_type, TraceJsonType::Missing);
    assert_eq!(null.state, TraceValueState::Null);
    assert_eq!(null.value_type, TraceJsonType::Null);
    assert_eq!(null.value, Some(json!(null)));
    assert_eq!(empty.state, TraceValueState::Present);
    assert_eq!(empty.value_type, TraceJsonType::String);
    assert_eq!(empty.value, Some(json!("")));
}

#[test]
fn trace_event_is_json_serializable() {
    let event = TraceEvent {
        id: 1,
        parent_id: None,
        kind: TraceEventKind::SourceRead,
        phase: TracePhase::Instant,
        rule_path: Some("mappings[0].source".to_string()),
        input_path: Some("@input.name".to_string()),
        output_path: None,
        namespace: Some("input".to_string()),
        operator: None,
        message: None,
        inputs: Vec::new(),
        output: Some(TraceValueSnapshot::from_json(
            &json!("alice"),
            &TransformTraceOptions::raw(),
            None,
        )),
        attributes: Default::default(),
    };

    let value = serde_json::to_value(event).expect("serialize trace event");
    assert_eq!(value["kind"], "source_read");
    assert_eq!(value["output"]["value"], "alice");
}

#[test]
fn trace_schema_serializes_stable_top_level_contract() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let raw = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("raw trace");
    let raw_value = serde_json::to_value(&raw.trace).expect("trace json");

    assert_eq!(raw_value["schema_version"], 1);
    assert_eq!(raw_value["value_mode"], "raw");
    assert_eq!(raw_value["contains_raw_values"], true);
    assert_eq!(raw_value["complete"], true);
    assert!(
        raw_value["records"]
            .as_array()
            .is_some_and(|records| !records.is_empty())
    );
    assert!(raw_value.get("finalize").is_none());
    assert_eq!(raw_value["records"][0]["events"][0]["kind"], "record_start");
    assert_eq!(raw_value["records"][0]["events"][0]["phase"], "instant");

    let redacted = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("redacted trace");
    let redacted_value = serde_json::to_value(&redacted.trace).expect("trace json");
    assert_eq!(redacted_value["value_mode"], "redacted");

    let metadata = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("metadata trace");
    let metadata_value = serde_json::to_value(&metadata.trace).expect("trace json");
    assert_eq!(metadata_value["value_mode"], "metadata_only");
    assert_eq!(metadata_value["contains_raw_values"], false);

    let finalize_yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
finalize:
  limit: 1
"#;
    let finalize_rule = parse_rule_file(finalize_yaml).expect("parse rule");
    let finalized = transform_input_with_trace(
        &finalize_rule,
        InputData::Text(r#"[{"name":"alice"},{"name":"bob"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("finalize trace");
    let finalized_value = serde_json::to_value(&finalized.trace).expect("trace json");
    assert!(finalized_value["finalize"].is_array());
    assert_eq!(finalized_value["finalize"][0]["kind"], "finalize_start");
    assert_eq!(finalized_value["finalize"][0]["phase"], "start");
}

#[test]
fn enabling_trace_does_not_change_transform_output() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"name":"alice"}]"#;

    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, normal);
    assert!(traced.warnings.is_empty());
    assert_eq!(traced.trace.records.len(), 1);
}
