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

#[test]
fn trace_v1_expression_operator_lifecycle() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "label"
    expr:
      op: "concat"
      args:
        - ref: "input.first"
        - " "
        - ref: "input.last"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"first":"Ada","last":"Lovelace"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_operator_lifecycle(&traced.trace, "concat");

    let text = serde_json::to_string(&traced.trace).unwrap();
    assert!(text.contains("Ada Lovelace"));
}

#[test]
fn trace_step_record_when_error_closes_open_spans() {
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - record_when: "@input.name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transform_input_with_trace(
            &rule,
            InputData::Text(r#"[{"name":"alice"}]"#),
            None,
            &TransformTraceOptions::raw(),
        )
    }));

    assert!(
        result.is_ok(),
        "record_when trace error must not leave an open span"
    );
    let err = result
        .expect("no panic")
        .expect_err("record_when should fail");
    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::ExprError);
}

#[test]
fn trace_assert_failure_uses_configured_error_message() {
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - asserts:
      - when:
          eq: ["@input.ok", true]
        error:
          code: "bad_input"
          message: "expected ok"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"ok":false}]"#;

    let normal = transform(&rule, input, None).expect_err("normal assertion error");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect_err("traced assertion error");

    assert_eq!(traced.error, normal);
    assert_eq!(
        traced.error.message,
        "assert failed: bad_input: expected ok"
    );
}

#[test]
fn trace_steps_record_when_asserts_and_finalize_as_spans() {
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - mappings:
      - target: "name"
        source: "name"
  - record_when:
      eq: ["@out.name", "alice"]
  - asserts:
      - when:
          eq: ["@out.name", "alice"]
        error:
          code: "bad_name"
          message: "bad name"
finalize:
  filter:
    eq: ["@item.name", "alice"]
  sort:
    by: "name"
    order: "asc"
  offset: 0
  limit: 1
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"},{"name":"bob"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!({"data":[{"name":"alice"}]}));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);

    let events = iter_trace_events(&traced.trace);
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::StepStart)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::AssertEval)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeFilter)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeSort)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeLimit)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeWrap)
    );

    let finalize_start = events
        .iter()
        .find(|event| event.kind == TraceEventKind::FinalizeStart)
        .expect("finalize start");
    for kind in [
        TraceEventKind::FinalizeFilter,
        TraceEventKind::FinalizeSort,
        TraceEventKind::FinalizeLimit,
        TraceEventKind::FinalizeWrap,
    ] {
        assert!(
            events
                .iter()
                .any(|event| event.kind == kind && event.parent_id == Some(finalize_start.id)),
            "missing child finalize event for {kind:?}"
        );
    }
}
