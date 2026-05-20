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
