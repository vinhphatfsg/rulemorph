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
