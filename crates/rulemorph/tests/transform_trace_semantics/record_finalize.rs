#[test]
fn source_path_helpers_preserve_context_out_and_escaped_input_paths() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "id"
    source: '["@id"]'
  - target: "tenant"
    source: "context.tenant"
  - target: "copied_id"
    source: "out.id"
"#;
    let rule = parse_rule(yaml);
    let context = json!({ "tenant": "acme" });
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"@id":"u1"}]"#),
        Some(&context),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([{ "id": "u1", "tenant": "acme", "copied_id": "u1" }])
    );
    assert_trace_shape(&traced.trace);
    let source_paths = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| event.kind == TraceEventKind::SourceRead)
        .filter_map(|event| event.input_path.as_deref())
        .collect::<Vec<_>>();
    assert!(source_paths.contains(&r#"@input["@id"]"#));
    assert!(source_paths.contains(&"@context.tenant"));
    assert!(source_paths.contains(&"@out.id"));
}

#[test]
fn trace_output_write_uses_output_snapshot_not_input_slot() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"name":"alice"}]"#);

    let output_write = iter_trace_events(&traced.trace)
        .into_iter()
        .find(|event| event.kind == TraceEventKind::OutputWrite)
        .expect("output_write event");
    assert!(
        output_write.inputs.is_empty(),
        "output_write must not put the written value in inputs"
    );
    assert_eq!(
        output_write
            .output
            .as_ref()
            .and_then(|snapshot| snapshot.value.as_ref()),
        Some(&json!("alice"))
    );
}

#[test]
fn trace_record_when_and_mapping_when_include_internal_condition_trace() {
    let yaml = r#"
version: 2
input:
  format: json
record_when:
  eq: ["@input.kind", "keep"]
mappings:
  - target: "name"
    when:
      eq: ["@input.enabled", true]
    source: "name"
"#;
    let rule = parse_rule(yaml);
    let traced =
        transform_text_raw_trace(&rule, r#"[{"kind":"keep","enabled":true,"name":"alice"}]"#);

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    let events = iter_trace_events(&traced.trace);
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@input.kind")
        }),
        "record_when should expose internal ref evaluation"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@input.enabled")
        }),
        "mapping.when should expose internal ref evaluation"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::ArgEval
                && event.operator.as_deref() == Some("eq")
                && attr_number(event, "arg_index") == Some(1)
        }),
        "condition comparison should expose actual argument evaluation"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_finalize_filter_and_sort_emit_item_level_decisions() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "id"
    source: "id"
  - target: "active"
    source: "active"
  - target: "score"
    source: "score"
finalize:
  filter:
    eq: ["@item.active", true]
  sort:
    by: "score"
    order: "desc"
"#;
    let rule = parse_rule(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(
            r#"[
                {"id":"a","active":true,"score":2},
                {"id":"b","active":false,"score":5},
                {"id":"c","active":true,"score":3}
            ]"#,
        ),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([
            {"id":"c","active":true,"score":3},
            {"id":"a","active":true,"score":2}
        ])
    );
    let finalize_events = traced.trace.finalize.as_ref().expect("finalize trace");
    assert!(
        finalize_events.iter().any(|event| {
            event.kind == TraceEventKind::FinalizeFilter
                && attr_number(event, "item_index") == Some(1)
                && attr_bool(event, "kept") == Some(false)
        }),
        "finalize.filter should emit per-item kept decisions"
    );
    assert!(
        finalize_events.iter().any(|event| {
            event.kind == TraceEventKind::FinalizeSort
                && attr_number(event, "from_index") == Some(1)
                && attr_number(event, "to_index") == Some(0)
        }),
        "finalize.sort should emit item movement decisions"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_record_api_matches_normal_finalize_behavior() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#;
    let rule = parse_rule(yaml);
    let record = json!({"name":"alice"});

    let normal = transform_record(&rule, &record, None).expect("normal record transform");
    let traced = transform_record_with_trace(&rule, &record, None, &TransformTraceOptions::raw())
        .expect("traced record transform");

    assert_eq!(normal, Some(json!({"data":[{"name":"alice"}]})));
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart),
        "single-record trace API must mirror normal finalize behavior"
    );
}

#[test]
fn trace_record_api_skips_finalize_when_record_is_dropped() {
    let yaml = r#"
version: 2
input:
  format: json
record_when: false
mappings:
  - target: "name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#;
    let rule = parse_rule(yaml);
    let record = json!({"name":"alice"});

    let normal = transform_record(&rule, &record, None).expect("normal record transform");
    let traced = transform_record_with_trace(&rule, &record, None, &TransformTraceOptions::raw())
        .expect("traced record transform");

    assert_eq!(normal, None);
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .all(|event| event.kind != TraceEventKind::FinalizeStart),
        "dropped record must not run finalize in trace mode"
    );
    assert_trace_shape(&traced.trace);
}
